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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.ExternalSorter

/**
 * Writes a map task's output into memory instead of to shuffle files.
 *
 * The record-processing pipeline is identical to the built-in
 * [[org.apache.spark.shuffle.sort.SortShuffleWriter]]: an `ExternalSorter` performs map-side
 * combine (when requested), key ordering, serialization/compression, and its own spill-to-disk
 * under memory pressure. The only difference is the sink -- output flows through the in-memory
 * [[MemoryShuffleExecutorComponents]] rather than the local-disk one, so the finished per-partition
 * bytes land in the [[MemoryShuffleBlockResolver]] instead of a shuffle file.
 *
 * Keeping the sorter means correctness (combine, ordering, large-input spill) matches sort shuffle
 * exactly; only the *final* materialized partitions are what this plugin holds in RAM.
 */
private[memory] class MemoryShuffleWriter[K, V, C](
    handle: BaseShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    shuffleExecutorComponents: ShuffleExecutorComponents)
    extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null
  // Map tasks may call stop(true) then stop(false) on exception; guard against double cleanup.
  private var stopping = false
  private var mapStatus: MapStatus = null
  private var partitionLengths: Array[Long] = _

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context,
        dep.aggregator,
        Some(dep.partitioner),
        dep.keyOrdering,
        dep.serializer,
        dep.rowBasedChecksums)
    } else {
      // No aggregator or ordering: per-partition key order, if needed, is established on the
      // reduce side (e.g. for sortByKey).
      new ExternalSorter[K, V, V](
        context,
        aggregator = None,
        Some(dep.partitioner),
        ordering = None,
        dep.serializer,
        dep.rowBasedChecksums)
    }
    sorter.insertAll(records)

    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      dep.shuffleId,
      mapId,
      dep.partitioner.numPartitions)
    sorter.writePartitionedMapOutput(dep.shuffleId, mapId, mapOutputWriter, writeMetrics)
    partitionLengths = mapOutputWriter.commitAllPartitions(sorter.getChecksums).getPartitionLengths
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) Option(mapStatus) else None
    } finally {
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime() - startTime)
        sorter = null
      }
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}
