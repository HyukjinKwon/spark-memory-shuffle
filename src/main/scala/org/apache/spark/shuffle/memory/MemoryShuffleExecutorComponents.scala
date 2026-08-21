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

import java.io.{BufferedOutputStream, ByteArrayOutputStream, File, FileOutputStream, OutputStream}
import java.util.{Map => JMap}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.storage.ShuffleBlockId

/**
 * In-memory implementation of the shuffle write plugin API ([[ShuffleExecutorComponents]]).
 *
 * This is the seam that lets [[MemoryShuffleWriter]] reuse Spark's `ExternalSorter` verbatim: the
 * sorter serializes, compresses/encrypts, sorts, aggregates, and (internally) spills its input
 * exactly as for the built-in sort shuffle, then streams each finished partition through the
 * [[ShufflePartitionWriter]] returned here. Those already-encoded bytes are captured and, at
 * commit, published to the [[MemoryShuffleBlockResolver]] as blocks -- kept in RAM when the storage
 * pool allows, spilled to disk otherwise. Since the bytes are already in on-wire shuffle format,
 * the standard reader consumes them unchanged.
 */
private[memory] class MemoryShuffleExecutorComponents(
    conf: SparkConf,
    resolver: MemoryShuffleBlockResolver)
    extends ShuffleExecutorComponents {

  private val perPartitionWriteBuffer = MemoryShuffleConf.perPartitionWriteBuffer(conf)

  override def initializeExecutor(
      appId: String,
      execId: String,
      extraConfigs: JMap[String, String]): Unit = {
    // Nothing to initialize: blocks live in this executor's heap/local-disk, addressed via the
    // resolver.
  }

  override def createMapOutputWriter(
      shuffleId: Int,
      mapTaskId: Long,
      numPartitions: Int): ShuffleMapOutputWriter = {
    new MemoryShuffleMapOutputWriter(
      resolver,
      shuffleId,
      mapTaskId,
      numPartitions,
      perPartitionWriteBuffer)
  }
}

/**
 * Collects one map task's partitioned output, one writer per reduce partition, and registers the
 * results with the resolver when the task commits.
 */
private[memory] class MemoryShuffleMapOutputWriter(
    resolver: MemoryShuffleBlockResolver,
    shuffleId: Int,
    mapId: Long,
    numPartitions: Int,
    perPartitionWriteBuffer: Long)
    extends ShuffleMapOutputWriter with Logging {

  // Lazily populated: a partition with no output keeps a null slot and contributes length 0.
  private val writers = new Array[MemoryShufflePartitionWriter](numPartitions)

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    val blockId = ShuffleBlockId(shuffleId, mapId, reducePartitionId)
    // Defer file creation until (and unless) the partition actually promotes to disk.
    val writer = new MemoryShufflePartitionWriter(
      perPartitionWriteBuffer,
      () => resolver.diskLocation(blockId))
    writers(reducePartitionId) = writer
    writer
  }

  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage = {
    val lengths = new Array[Long](numPartitions)
    var p = 0
    while (p < numPartitions) {
      val writer = writers(p)
      if (writer != null) {
        writer.finishWriting()
        val len = writer.numBytesWritten
        lengths(p) = len
        if (len > 0L) {
          val blockId = ShuffleBlockId(shuffleId, mapId, p)
          if (writer.spilledToDisk) {
            resolver.putOnDisk(blockId, writer.diskFile, len)
          } else {
            resolver.putInMemory(blockId, writer.inMemoryBytes)
          }
        }
      }
      p += 1
    }
    MapOutputCommitMessage.of(lengths)
  }

  override def abort(error: Throwable): Unit = {
    var p = 0
    while (p < numPartitions) {
      val writer = writers(p)
      if (writer != null) {
        writer.abort()
        writers(p) = null
      }
      p += 1
    }
  }
}

/**
 * A [[ShufflePartitionWriter]] that buffers a partition's bytes in the heap up to
 * `perPartitionWriteBuffer`, then promotes to a local file and streams the remainder to disk. This
 * caps the transient heap footprint of any single partition, so even a badly skewed partition
 * cannot OOM the map task; retention of the finished block is then bounded separately by the
 * resolver's storage-memory accounting.
 */
private[memory] class MemoryShufflePartitionWriter(
    threshold: Long,
    diskFileProvider: () => File)
    extends ShufflePartitionWriter {

  private var heapBuffer: ByteArrayOutputStream = new ByteArrayOutputStream()
  private var fileOut: OutputStream = null
  private var file: File = null
  private var count = 0L
  private var closed = false

  private val stream: OutputStream = new OutputStream {
    override def write(b: Int): Unit = {
      promoteIfNeeded(1)
      if (fileOut != null) fileOut.write(b) else heapBuffer.write(b)
      count += 1L
    }

    override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
      promoteIfNeeded(len)
      if (fileOut != null) fileOut.write(bytes, off, len) else heapBuffer.write(bytes, off, len)
      count += len.toLong
    }

    override def flush(): Unit = if (fileOut != null) fileOut.flush()

    // ExternalSorter closes this stream when the partition is done; keep the accumulated bytes
    // (heap) or the finished file (disk) available for commit.
    override def close(): Unit = finishWriting()
  }

  private def promoteIfNeeded(additional: Int): Unit = {
    if (fileOut == null && count + additional > threshold) {
      file = diskFileProvider()
      fileOut = new BufferedOutputStream(new FileOutputStream(file))
      heapBuffer.writeTo(fileOut)
      heapBuffer = null
    }
  }

  override def openStream(): OutputStream = stream

  override def getNumBytesWritten(): Long = count

  def numBytesWritten: Long = count

  def spilledToDisk: Boolean = fileOut != null

  def diskFile: File = file

  def inMemoryBytes: Array[Byte] = {
    require(!spilledToDisk, "partition was streamed to disk; call diskFile instead")
    heapBuffer.toByteArray
  }

  /** Idempotently finish writing: flush and close the disk stream if the partition promoted. */
  def finishWriting(): Unit = {
    if (!closed) {
      closed = true
      if (fileOut != null) {
        fileOut.flush()
        fileOut.close()
      }
    }
  }

  /** Discard partial output for an aborted map task. */
  def abort(): Unit = {
    finishWriting()
    if (file != null && file.exists()) {
      file.delete()
    }
    heapBuffer = null
  }
}
