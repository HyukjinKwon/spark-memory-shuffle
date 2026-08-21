/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.memory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/**
 * End-to-end benchmark comparing the built-in SortShuffleManager against MemoryShuffleManager on a
 * real `local-cluster` (multiple executor JVMs, so shuffle blocks are fetched over the network).
 * Not a unit test -- run explicitly:
 *
 * {{{
 *   sbt 'Test/runMain org.apache.spark.shuffle.memory.MemoryShuffleBenchmark'
 * }}}
 *
 * Running it *reliably* -- reading the source before trusting a number:
 *
 *  - Wall-clock alone is misleading on a single machine: the OS page cache turns the built-in
 *    manager's "disk" shuffle files into RAM, so there is no disk cost to remove and both managers
 *    look identical. This harness therefore also reports the shuffle metrics that isolate the
 *    shuffle itself -- aggregate shuffle *write time* and *fetch wait time* from every task, via a
 *    SparkListener -- which move even when total wall-clock does not.
 *  - Each manager runs in its own JVM set with warmup iterations (JIT) before timed ones, and we
 *    report median / min / stdev over several iterations so noise is visible rather than hidden.
 *  - To make local-disk cost actually observable (and see MemoryShuffleManager win), point the
 *    built-in shuffle at slow/throttled storage via -Dbench.localDir=/path and compare against the
 *    same on a fast/tmpfs dir, or run on a real multi-node cluster. On one box with a fast SSD and
 *    a warm page cache, parity is the honest expected result.
 *
 * Tunable via -D system properties: bench.executors, bench.cores, bench.execMemMb, bench.rows,
 * bench.partitions, bench.reducers, bench.valueSize, bench.warmup, bench.iters, bench.workload
 * (groupByKey | sortByKey | repartition), bench.localDir.
 */
object MemoryShuffleBenchmark {

  private def prop(k: String, d: String): String = System.getProperty(k, d)

  private val executors = prop("bench.executors", "2").toInt
  private val cores = prop("bench.cores", "2").toInt
  private val execMemMb = prop("bench.execMemMb", "2048").toInt
  private val rows = prop("bench.rows", "8000000").toLong
  private val partitions = prop("bench.partitions", "32").toInt
  private val reducers = prop("bench.reducers", "64").toInt
  private val valueSize = prop("bench.valueSize", "64").toInt
  private val warmup = prop("bench.warmup", "2").toInt
  private val iters = prop("bench.iters", "5").toInt
  private val workload = prop("bench.workload", "groupByKey")
  private val localDir = prop("bench.localDir", "")

  private val driverClassPath = System.getProperty("java.class.path")

  /** Aggregates shuffle-related task metrics so the shuffle cost can be read in isolation. */
  private class ShuffleMetricsListener extends SparkListener {
    private val lock = new Object
    private var writeNs = 0L
    private var fetchMs = 0L
    private var remoteBytes = 0L
    private var runMs = 0L

    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
      val m = e.taskMetrics
      if (m != null) lock.synchronized {
        writeNs += m.shuffleWriteMetrics.writeTime
        fetchMs += m.shuffleReadMetrics.fetchWaitTime
        remoteBytes += m.shuffleReadMetrics.remoteBytesRead
        runMs += m.executorRunTime
      }
    }

    /** (shuffleWriteMs, fetchWaitMs, remoteMB, executorRunMs) since the last reset, then reset. */
    def snapshotAndReset(): (Double, Double, Double, Double) = lock.synchronized {
      val r = (writeNs / 1e6, fetchMs.toDouble, remoteBytes / (1024.0 * 1024.0), runMs.toDouble)
      writeNs = 0L; fetchMs = 0L; remoteBytes = 0L; runMs = 0L
      r
    }
  }

  private case class Result(
      wall: Seq[Double],
      shuffleWriteMsPerIter: Double,
      fetchWaitMsPerIter: Double,
      remoteMbPerIter: Double,
      execRunMsPerIter: Double)

  def main(args: Array[String]): Unit = {
    if (System.getProperty("spark.home") == null && sys.env.get("SPARK_HOME").isEmpty) {
      System.setProperty("spark.home", System.getProperty("user.dir"))
    }

    val approxMb = rows * (valueSize + 12) / (1024 * 1024)
    println(f"""
      |==================== MemoryShuffle E2E benchmark ====================
      | cluster        : local-cluster[$executors, $cores, ${execMemMb}m]
      | workload       : $workload
      | rows           : $rows  (value ~$valueSize bytes)
      | partitions     : $partitions map -> $reducers reduce
      | approx shuffle : ~$approxMb MB
      | warmup / iters : $warmup / $iters
      | local dir      : ${if (localDir.isEmpty) "(default)" else localDir}
      |=====================================================================
      |""".stripMargin)

    val sort = run("SortShuffleManager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    val mem = run("MemoryShuffleManager", classOf[MemoryShuffleManager].getName)

    def line(label: String, r: Result): Unit = {
      val runs = r.wall.map(t => f"$t%.3f").mkString(", ")
      println(f" $label%-20s wall med ${median(r.wall)}%6.3fs  min ${r.wall.min}%6.3fs  " +
        f"sd ${stdev(r.wall)}%.3f  | shuffleWrite ${r.shuffleWriteMsPerIter}%7.1fms  " +
        f"fetchWait ${r.fetchWaitMsPerIter}%7.1fms  remote ${r.remoteMbPerIter}%6.1fMB")
      println(f"   ${" " * 18} runs: [$runs]")
    }

    println("\n==================== RESULTS (per iteration) ====================")
    line("SortShuffleManager", sort)
    line("MemoryShuffleManager", mem)
    val sm = median(sort.wall)
    val mm = median(mem.wall)
    println(
      f"\n wall-clock speedup (sort/mem) : ${sm / mm}%.2fx   (${(sm - mm) / sm * 100}%+.1f%%)")
    println(f" shuffle-write-time  (sort/mem) : " +
      f"${sort.shuffleWriteMsPerIter / math.max(mem.shuffleWriteMsPerIter, 1e-9)}%.2fx  " +
      f"(${sort.shuffleWriteMsPerIter}%.0fms vs ${mem.shuffleWriteMsPerIter}%.0fms)")
    println("=================================================================")
  }

  private def run(label: String, manager: String): Result = {
    val conf = new SparkConf()
      .setMaster(s"local-cluster[$executors,$cores,$execMemMb]")
      .setAppName(s"bench-$label")
      .set("spark.shuffle.manager", manager)
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.ui.enabled", "false")
      .set("spark.executor.memory", s"${execMemMb}m")
      .set("spark.executor.extraClassPath", driverClassPath)
      .set("spark.driver.extraClassPath", driverClassPath)
    if (localDir.nonEmpty) conf.set("spark.local.dir", localDir)

    val sc = new SparkContext(conf)
    val listener = new ShuffleMetricsListener
    try {
      val deadline = System.currentTimeMillis() + 30000
      while (sc.getExecutorIds().size < executors && System.currentTimeMillis() < deadline) {
        Thread.sleep(200)
      }
      (0 until warmup).foreach(_ => timeOnce(sc))

      sc.addSparkListener(listener)
      listener.snapshotAndReset()
      val wall = (0 until iters).map(_ => timeOnce(sc))
      Thread.sleep(1000) // let the async listener bus drain before snapshotting metrics
      val (writeMs, fetchMs, remoteMb, runMs) = listener.snapshotAndReset()

      val res = Result(wall, writeMs / iters, fetchMs / iters, remoteMb / iters, runMs / iters)
      println(f"  [$label%-20s] wall (s): ${wall.map(t => f"$t%.3f").mkString(", ")}")
      res
    } finally {
      sc.stop()
    }
  }

  /** Run one shuffle job and return its wall-clock seconds. */
  private def timeOnce(sc: SparkContext): Double = {
    val start = System.nanoTime()
    val base = sc.range(0, rows, 1, partitions).map { i =>
      (i % reducers, (i.toString + "x" * valueSize).take(valueSize))
    }
    val result: Long = workload match {
      case "sortByKey" =>
        base.sortByKey(numPartitions = reducers).count()
      case "repartition" =>
        base.repartition(reducers).count()
      case _ => // groupByKey
        base.groupByKey(reducers).mapValues(_.size).values.map(_.toLong).sum().toLong
    }
    if (result < 0) throw new IllegalStateException("negative")
    (System.nanoTime() - start) / 1e9
  }

  private def median(xs: Seq[Double]): Double = {
    val s = xs.sorted
    val n = s.length
    if (n == 0) Double.NaN
    else if (n % 2 == 1) s(n / 2)
    else (s(n / 2 - 1) + s(n / 2)) / 2.0
  }

  private def stdev(xs: Seq[Double]): Double = {
    if (xs.length < 2) return 0.0
    val mean = xs.sum / xs.length
    math.sqrt(xs.map(x => (x - mean) * (x - mean)).sum / (xs.length - 1))
  }
}
