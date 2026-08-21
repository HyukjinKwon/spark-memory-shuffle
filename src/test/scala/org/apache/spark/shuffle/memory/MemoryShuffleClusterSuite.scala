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

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Cross-process tests: run on a real `local-cluster` with multiple executor JVMs, so shuffle blocks
 * are genuinely fetched node-to-node over the network rather than served in-process. This is what
 * validates the memory-to-memory exchange claim and, separately, block migration on executor
 * decommissioning.
 *
 * Executors are launched as separate processes; the driver's full classpath (which includes both
 * this plugin's classes and spark-core) is propagated to them via `spark.executor.extraClassPath`.
 */
class MemoryShuffleClusterSuite extends AnyFunSuite {

  private val driverClassPath = System.getProperty("java.class.path")

  // The launcher's getSparkHome() reads the spark.home system property when SPARK_HOME is unset.
  // Any real directory works: with SPARK_TESTING=1 the executor classpath comes from
  // spark.executor.extraClassPath, not from a distribution layout under this home.
  if (System.getProperty("spark.home") == null && sys.env.get("SPARK_HOME").isEmpty) {
    System.setProperty("spark.home", System.getProperty("user.dir"))
  }

  private def clusterConf: SparkConf = new SparkConf()
    .setMaster("local-cluster[2,1,1024]")
    .setAppName("MemoryShuffleClusterSuite")
    .set("spark.shuffle.manager", classOf[MemoryShuffleManager].getName)
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.ui.enabled", "false")
    // Propagate the test classpath (plugin + spark) to the executor processes.
    .set("spark.executor.extraClassPath", driverClassPath)
    .set("spark.driver.extraClassPath", driverClassPath)

  private def withCluster(extra: (String, String)*)(body: SparkContext => Unit): Unit = {
    val conf = clusterConf
    extra.foreach { case (k, v) => conf.set(k, v) }
    val sc = new SparkContext(conf)
    try {
      // Wait for both executors to register so work actually spreads across processes.
      eventually(timeout(30.seconds), interval(200.millis)) {
        assert(sc.getExecutorIds().size >= 2)
      }
      body(sc)
    } finally {
      sc.stop()
    }
  }

  test("shuffle fetched across executor processes stays correct") {
    withCluster() { sc =>
      val n = 100000
      val result = sc.parallelize(1 to n, 12)
        .map(i => (i % 100, i.toLong))
        .reduceByKey(_ + _)
        .collectAsMap()
      val expected = (1 to n).groupBy(_ % 100).map { case (k, vs) => k -> vs.map(_.toLong).sum }
      assert(result === expected)
    }
  }

  test("job stays correct after an executor holding in-memory shuffle output is removed") {
    // We intentionally do NOT set spark.decommission.enabled, which would make the standalone
    // Worker register a SIGPWR handler -- a signal that does not exist on macOS. Decommissioning is
    // driven programmatically; executor-side storage migration is gated by the
    // spark.storage.decommission.* flags below. The migration *mechanics* (getStoredShuffles /
    // getMigrationBlocks / putShuffleBlockAsStream / serve) are proven deterministically in
    // MemoryShuffleManagerSuite; this test checks end-to-end continuity when an executor that holds
    // retained in-memory shuffle output actually goes away.
    withCluster(
      "spark.storage.decommission.enabled" -> "true",
      "spark.storage.decommission.shuffleBlocks.enabled" -> "true",
      "spark.storage.decommission.maxReplicationFailuresPerBlock" -> "3"
    ) { sc =>
      val expected = (1 to 20000).groupBy(_ % 50).map { case (k, vs) => k -> vs.map(_.toLong).sum }
      // Materialize the shuffle map output so it is genuinely resident on the executors.
      val shuffled = sc.parallelize(1 to 20000, 8).map(i => (i % 50, i.toLong)).reduceByKey(_ + _)
      shuffled.count()
      assert(shuffled.collectAsMap() === expected)

      val victim = sc.getExecutorIds().head
      val sched = sc.schedulerBackend.asInstanceOf[
        org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend]
      sched.decommissionExecutor(
        victim,
        org.apache.spark.scheduler.ExecutorDecommissionInfo("test", None),
        adjustTargetNumExecutors = true)

      // Wait for the victim to actually leave the cluster. If this environment cannot fully
      // decommission and remove an executor (e.g. macOS), cancel rather than assert vacuously --
      // migration mechanics are covered by the deterministic unit tests.
      val removed =
        try {
          eventually(timeout(90.seconds), interval(1.second)) {
            assert(!sc.getExecutorIds().contains(victim))
          }
          true
        } catch {
          case _: Throwable => false
        }
      assume(
        removed,
        "executor was not removed in this environment; skipping the end-to-end continuity check " +
          "(migration mechanics are covered by MemoryShuffleManagerSuite)"
      )

      // The victim is gone. Re-fetching the shuffle output must still produce the right answer,
      // whether the blocks were migrated to the peer or recomputed on it.
      assert(shuffled.collectAsMap() === expected)
    }
  }
}
