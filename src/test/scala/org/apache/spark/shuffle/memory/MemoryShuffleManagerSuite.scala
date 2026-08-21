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

import java.nio.ByteBuffer

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.{ShuffleBlockId, ShuffleIndexBlockId}

/**
 * End-to-end tests: run real shuffles on a local SparkContext configured to use
 * [[MemoryShuffleManager]], and assert the results match.
 *
 * Every operation is exercised twice via [[forEachMode]]:
 *   - "in-memory": the default, where finished partitions are retained in RAM;
 *   - "forced-disk": `perPartitionWriteBufferBytes = 1`, so every partition promotes to a local
 *     file on the first byte -- exercising the write-time spill, `FileSegmentManagedBuffer`
 *     serving, and file cleanup paths.
 */
class MemoryShuffleManagerSuite extends AnyFunSuite {

  private def baseConf: SparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("MemoryShuffleManagerSuite")
    .set("spark.shuffle.manager", classOf[MemoryShuffleManager].getName)
    // The ESS cannot serve memory-only blocks; keep it off.
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.ui.enabled", "false")

  private def withContext(extra: (String, String)*)(body: SparkContext => Unit): Unit = {
    val conf = baseConf
    extra.foreach { case (k, v) => conf.set(k, v) }
    val sc = new SparkContext(conf)
    try {
      body(sc)
    } finally {
      sc.stop()
    }
  }

  /** Run `body` under both the in-memory and the forced-disk configurations. */
  private def forEachMode(name: String)(body: SparkContext => Unit): Unit = {
    test(s"$name (in-memory)") {
      withContext()(body)
    }
    test(s"$name (forced-disk)") {
      withContext(MemoryShuffleConf.PER_PARTITION_WRITE_BUFFER -> "1")(body)
    }
  }

  forEachMode("reduceByKey with map-side combine") { sc =>
    val result = sc.parallelize(1 to 1000, 8)
      .map(i => (i % 10, i))
      .reduceByKey(_ + _)
      .collectAsMap()
    val expected = (1 to 1000).groupBy(_ % 10).map { case (k, vs) => k -> vs.sum }
    assert(result === expected)
  }

  forEachMode("groupByKey preserves all values") { sc =>
    val result = sc.parallelize(1 to 100, 4)
      .map(i => (i % 5, i))
      .groupByKey()
      .mapValues(_.toSet)
      .collectAsMap()
    val expected = (1 to 100).groupBy(_ % 5).map { case (k, vs) => k -> vs.toSet }
    assert(result === expected)
  }

  forEachMode("sortByKey across partitions") { sc =>
    val sorted = sc.parallelize(Seq(5, 3, 9, 1, 7, 2, 8, 4, 6, 0), 4)
      .map(i => (i, i))
      .sortByKey()
      .keys
      .collect()
    assert(sorted.toSeq === (0 to 9))
  }

  forEachMode("join through the memory shuffle") { sc =>
    val a = sc.parallelize(Seq((1, "a"), (2, "b"), (3, "c")), 3)
    val b = sc.parallelize(Seq((1, "x"), (2, "y"), (2, "z")), 3)
    val joined = a.join(b).collect().toSet
    assert(joined === Set((1, ("a", "x")), (2, ("b", "y")), (2, ("b", "z"))))
  }

  forEachMode("empty shuffle") { sc =>
    val result = sc.parallelize(Seq.empty[Int], 4)
      .map(i => (i, i))
      .reduceByKey(_ + _)
      .collect()
    assert(result.isEmpty)
  }

  forEachMode("two chained shuffles") { sc =>
    val result = sc.parallelize(1 to 500, 6)
      .map(i => (i % 7, 1))
      .reduceByKey(_ + _) // shuffle 1
      .map { case (k, c) => (c, k) }
      .sortByKey() // shuffle 2
      .collect()
    val expectedPairs = (1 to 500).groupBy(_ % 7)
      .toSeq.map { case (k, vs) => (vs.size, k) }.toSet
    // sortByKey orders by count only; tie order among equal counts is unspecified, so assert the
    // keys are non-decreasing and the set of pairs is exactly right.
    assert(result.map(_._1).toSeq === result.map(_._1).toSeq.sorted)
    assert(result.toSet === expectedPairs)
  }

  test("spills to disk under storage-memory pressure and stays correct") {
    // Shrink the memory manager's view of the heap so the storage pool is tiny (~19 MiB) while the
    // real test JVM heap is large. Retained shuffle output then exceeds the pool, forcing
    // acquireStorageMemory to fail and the resolver to spill blocks to disk -- the "about to OOM"
    // path -- without the JVM actually running out of memory. Compression off keeps retained bytes
    // close to raw size so the pool fills predictably.
    withContext(
      "spark.testing.memory" -> (32L * 1024 * 1024).toString,
      "spark.testing.reservedMemory" -> "0",
      "spark.shuffle.compress" -> "false") { sc =>
      val n = 500000
      val numKeys = 50
      val payload = "x" * 48
      val sizes = sc.parallelize(0 until n, 16)
        .map(i => (i % numKeys, s"$i-$payload"))
        .groupByKey()
        .mapValues(_.size)
        .collectAsMap()

      val expected = (0 until n).groupBy(_ % numKeys).map { case (k, vs) => k -> vs.size }
      assert(sizes === expected)

      // In local mode driver and executor share one SparkEnv, so the resolver counter is visible.
      val resolver = SparkEnv.get.shuffleManager
        .asInstanceOf[MemoryShuffleManager].shuffleBlockResolver
      assert(
        resolver.numMemoryPressureSpills > 0,
        s"expected some blocks to spill under memory pressure, " +
          s"got ${resolver.numMemoryPressureSpills}")
    }
  }

  test("larger volume with forced disk spill stays correct") {
    withContext(MemoryShuffleConf.PER_PARTITION_WRITE_BUFFER -> "1") { sc =>
      val n = 200000
      val sum = sc.parallelize(1 to n, 16)
        .map(i => (i % 1000, i.toLong))
        .reduceByKey(_ + _)
        .values
        .sum()
      assert(sum === (1L to n.toLong).sum)
    }
  }

  private def resolverOf(sc: SparkContext): MemoryShuffleBlockResolver =
    SparkEnv.get.shuffleManager.asInstanceOf[MemoryShuffleManager].shuffleBlockResolver

  private def bytesOf(buffer: ManagedBuffer): Array[Byte] = {
    val bb = buffer.nioByteBuffer()
    val arr = new Array[Byte](bb.remaining())
    bb.get(arr)
    arr
  }

  test("MigratableResolver exposes stored shuffles and per-partition migration blocks") {
    withContext() { sc =>
      // Populate the resolver with a real shuffle, kept resident by materializing the result.
      sc.parallelize(1 to 1000, 6).map(i => (i % 8, i)).reduceByKey(_ + _).collect()
      val resolver = resolverOf(sc)

      val stored = resolver.getStoredShuffles()
      assert(stored.nonEmpty, "expected the resolver to report stored shuffle maps")

      val info = stored.head
      val migration = resolver.getMigrationBlocks(info)
      assert(migration.nonEmpty)
      // Data blocks (ShuffleBlockId) first, exactly one ShuffleIndexBlockId marker, and it is last.
      val (last, rest) = (migration.last, migration.dropRight(1))
      assert(
        last._1.isInstanceOf[ShuffleIndexBlockId],
        s"expected index marker last, got ${last._1}")
      assert(rest.nonEmpty && rest.forall(_._1.isInstanceOf[ShuffleBlockId]))
      assert(migration.count(_._1.isInstanceOf[ShuffleIndexBlockId]) == 1)

      // A migration data buffer must equal what the resolver serves for that same block.
      val (dataId, dataBuf) = rest.head
      assert(bytesOf(dataBuf).sameElements(bytesOf(resolver.getBlockData(dataId))))
    }
  }

  test("putShuffleBlockAsStream stores a received block so it can be served") {
    withContext() { sc =>
      val resolver = resolverOf(sc)
      // A block id unrelated to any live shuffle, so we do not disturb running state.
      val received = ShuffleBlockId(987654, 0L, 0)
      val payload = ("migrated-payload-" * 64).getBytes("UTF-8")

      val callback = resolver.putShuffleBlockAsStream(received, SparkEnv.get.serializerManager)
      callback.onData(received.name, ByteBuffer.wrap(payload))
      callback.onComplete(received.name)

      assert(bytesOf(resolver.getBlockData(received)).sameElements(payload))
      // And it now shows up as a stored shuffle available for onward migration.
      assert(resolver.getStoredShuffles().exists(_.shuffleId == 987654))
    }
  }
}
