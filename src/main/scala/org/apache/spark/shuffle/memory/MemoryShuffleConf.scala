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

import org.apache.spark.SparkConf

/**
 * Configuration for the in-memory shuffle manager.
 *
 * Retention memory (how much shuffle output is kept in RAM) is NOT a separate budget: it is drawn
 * from Spark's unified storage-memory pool via the `MemoryManager`, so it is bounded by
 * `spark.memory.fraction` / `spark.memory.storageFraction` like any other storage. When the pool
 * cannot grant a block, the block spills to local disk. The one knob below bounds the *transient*
 * per-partition write buffer so a single large (e.g. skewed) partition streams to disk instead of
 * being buffered whole in the heap.
 */
private[memory] object MemoryShuffleConf {

  /**
   * Per-partition write-buffer cap, in bytes. While a map task writes one reduce partition's
   * output, bytes accumulate in a heap buffer up to this size; beyond it, that partition's writer
   * promotes to a local file and streams the remainder to disk. This bounds the worst-case heap
   * spike from any single partition regardless of data skew.
   */
  val PER_PARTITION_WRITE_BUFFER = "spark.shuffle.memory.perPartitionWriteBufferBytes"

  /** Default per-partition write-buffer cap: 16 MiB. */
  val PER_PARTITION_WRITE_BUFFER_DEFAULT: Long = 16L * 1024 * 1024

  def perPartitionWriteBuffer(conf: SparkConf): Long =
    conf.getSizeAsBytes(PER_PARTITION_WRITE_BUFFER, PER_PARTITION_WRITE_BUFFER_DEFAULT.toString)
}
