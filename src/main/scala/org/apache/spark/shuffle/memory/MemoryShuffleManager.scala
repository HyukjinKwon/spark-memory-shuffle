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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{BaseShuffleHandle, BlockStoreShuffleReader, ShuffleHandle}
import org.apache.spark.shuffle.{ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}

/**
 * A [[ShuffleManager]] that keeps shuffle output in executor memory and exchanges it directly
 * between nodes over the block transfer service -- no shuffle files on the happy path.
 *
 * The design deliberately keeps Spark's classic map/reduce *decoupling*: a map task fully
 * materializes its output (in RAM, via [[MemoryShuffleWriter]]) and registers a `MapStatus`, and
 * reducers pull blocks afterwards through the ordinary
 * [[org.apache.spark.shuffle.BlockStoreShuffleReader]] + `MapOutputTracker` path. Only the storage
 * medium changes, so fault tolerance and the read transport are unaffected. This is the intended
 * shape for the "spill to files past a threshold" extension: crossing the memory budget flips
 * individual blocks to disk-backed buffers inside [[MemoryShuffleBlockResolver]] without touching
 * this class or the reader.
 *
 * Enable with:
 * {{{
 *   spark.shuffle.manager = org.apache.spark.shuffle.memory.MemoryShuffleManager
 * }}}
 *
 * SparkEnv instantiates this via the (SparkConf) constructor. The isDriver-aware two-arg form is
 * also accepted by Spark's reflective loader; a single-arg constructor is sufficient here because
 * the manager holds no driver-only state.
 */
private[spark] class MemoryShuffleManager(conf: SparkConf)
    extends ShuffleManager with Logging {

  logInfo("Using MemoryShuffleManager: shuffle output is retained in memory")

  private val resolver = new MemoryShuffleBlockResolver(conf)
  private val executorComponents = new MemoryShuffleExecutorComponents(conf, resolver)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val base = handle.asInstanceOf[BaseShuffleHandle[K, V, Any]]
    new MemoryShuffleWriter(base, mapId, context, metrics, executorComponents)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // Reuse Spark's block-store reader unchanged: it fetches blocks by BlockId through the block
    // manager, which routes back to our resolver. Push-based merge is not used, so we always take
    // the plain getMapSizesByExecutorId path.
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition)
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      blocksByAddress,
      context,
      metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    resolver.removeShuffle(shuffleId)
    true
  }

  override def shuffleBlockResolver: MemoryShuffleBlockResolver = resolver

  override def stop(): Unit = resolver.stop()
}
