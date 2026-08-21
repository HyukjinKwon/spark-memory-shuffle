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

import java.io.{File, FileOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{MigratableResolver, ShuffleBlockInfo, ShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, BlockStatus, ShuffleBlockId, ShuffleIndexBlockId, ShuffleMergedBlockId, StorageLevel}

/**
 * Stores and serves shuffle blocks, keeping them in executor memory when the unified storage-memory
 * pool has room and spilling to local disk when it does not.
 *
 * Memory model: for each in-memory block the resolver reserves `size` bytes of ON_HEAP storage
 * memory through the [[org.apache.spark.memory.MemoryManager]] -- the same pool used by cached RDD
 * blocks -- and releases it when the shuffle is cleaned up. If the pool cannot grant a block (full
 * even after evicting evictable storage), that block is written to a local file and served as a
 * [[FileSegmentManagedBuffer]]. Because reservations never exceed what the pool grants, retained
 * shuffle output can never drive the heap past the storage pool's ceiling; the executor spills
 * instead of OOMing.
 *
 * Cooperativeness note: these reservations are pinned -- the MemoryManager will not evict a
 * retained shuffle block to satisfy another request (the block is not registered with the
 * MemoryStore's eviction callback). Execution memory therefore treats retained shuffle output as
 * unreclaimable and adapts by spilling its own state, which is safe (no OOM), just less elastic
 * than cached blocks. This is the deliberate trade for keeping the read path a drop-in.
 *
 * External Shuffle Service: memory-resident blocks cannot be served by the ESS after an executor
 * dies, so a memory shuffle deployment should disable it and accept map-stage recomputation on
 * executor loss. Disk-spilled blocks live under the executor's local dirs and share that fate.
 *
 * Executor decommissioning: the resolver implements [[MigratableResolver]], so a decommissioned
 * executor's retained shuffle blocks are migrated to a peer rather than lost. The in-memory format
 * has no index file, so migration sends each per-partition block followed by an index-block marker;
 * the marker's status report on the receiving side drives the driver to re-point this map's
 * reducers at the new executor (see `BlockManagerMasterEndpoint`).
 */
private[memory] class MemoryShuffleBlockResolver(conf: SparkConf)
    extends ShuffleBlockResolver with MigratableResolver with Logging {

  private sealed trait StoredBlock {
    def size: Long
    def toManagedBuffer: ManagedBuffer

    /** Bytes of ON_HEAP storage memory reserved for this block (0 for disk-resident blocks). */
    def reservedMemory: Long
  }

  private final class InMemoryBlock(bytes: Array[Byte]) extends StoredBlock {
    override val size: Long = bytes.length.toLong
    // A fresh NioManagedBuffer wraps the same array without copying on each fetch.
    override def toManagedBuffer: ManagedBuffer = new NioManagedBuffer(ByteBuffer.wrap(bytes))
    override def reservedMemory: Long = size
  }

  private final class OnDiskBlock(val file: File, override val size: Long) extends StoredBlock {
    override def toManagedBuffer: ManagedBuffer =
      new FileSegmentManagedBuffer(transportConf, file, 0L, size)
    override def reservedMemory: Long = 0L
  }

  private val blocks = new ConcurrentHashMap[ShuffleBlockId, StoredBlock]()

  // Bytes of storage memory this resolver currently holds; used only for observability/asserts.
  private val reservedBytes = new AtomicLong(0L)

  // Count of blocks spilled to disk because the storage pool could not grant them (memory
  // pressure), as opposed to partitions the write path streamed to disk up front. Exposed for
  // tests/metrics.
  private val memoryPressureSpills = new AtomicLong(0L)

  /** Number of blocks spilled to disk due to storage-pool exhaustion (memory pressure). */
  def numMemoryPressureSpills: Long = memoryPressureSpills.get()

  /** Bytes of ON_HEAP storage memory currently reserved for retained in-memory blocks. */
  def reservedMemoryBytes: Long = reservedBytes.get()

  private val transportConf: TransportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  // Resolved lazily: SparkEnv is not fully initialized while the ShuffleManager is being
  // constructed, but these are only touched during task execution.
  private lazy val memoryManager = SparkEnv.get.memoryManager
  private lazy val blockManager = SparkEnv.get.blockManager
  private lazy val diskBlockManager = blockManager.diskBlockManager

  // Shuffles whose blocks should not be offered for migration (already migrated away).
  private val shufflesToSkip = ConcurrentHashMap.newKeySet[java.lang.Integer]()

  // Reduce id used for the synthetic index-block marker during migration (mirrors Spark's
  // IndexShuffleBlockResolver.NOOP_REDUCE_ID).
  private val NOOP_REDUCE_ID = 0

  /** The on-disk location this block would occupy if spilled. Deterministic, unique per block. */
  def diskLocation(blockId: ShuffleBlockId): File = diskBlockManager.getFile(blockId)

  /**
   * Store a block whose bytes are in memory. Reserves storage memory and keeps it in RAM if the
   * pool allows; otherwise spills the bytes to [[diskLocation]] and serves from disk.
   */
  def putInMemory(blockId: ShuffleBlockId, bytes: Array[Byte]): Unit = {
    val len = bytes.length.toLong
    if (len == 0L) {
      return
    }
    if (memoryManager.acquireStorageMemory(blockId, len, MemoryMode.ON_HEAP)) {
      putStored(blockId, new InMemoryBlock(bytes))
      reservedBytes.addAndGet(len)
    } else {
      val file = diskLocation(blockId)
      writeBytesToFile(bytes, file)
      putStored(blockId, new OnDiskBlock(file, len))
      memoryPressureSpills.incrementAndGet()
      logDebug(s"Spilled $blockId ($len bytes) to disk: storage memory pool full")
    }
  }

  /**
   * Register a block whose bytes have already been streamed to `file` (a large partition that the
   * write path sent straight to disk). No memory is reserved.
   */
  def putOnDisk(blockId: ShuffleBlockId, file: File, length: Long): Unit = {
    if (length == 0L) {
      if (file.exists()) file.delete()
      return
    }
    putStored(blockId, new OnDiskBlock(file, length))
  }

  private def putStored(blockId: ShuffleBlockId, block: StoredBlock): Unit = {
    val previous = blocks.put(blockId, block)
    if (previous != null) {
      // Extremely unlikely (unique block ids), but stay consistent if a retry re-writes a block.
      freeBlock(previous)
    }
  }

  private def writeBytesToFile(bytes: Array[Byte], file: File): Unit = {
    val out = new FileOutputStream(file)
    try {
      out.write(bytes)
    } catch {
      case e: IOException =>
        out.close()
        throw new IOException(s"Failed to spill shuffle block to $file", e)
    }
    out.close()
  }

  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case shuffleBlockId: ShuffleBlockId =>
        val block = blocks.get(shuffleBlockId)
        if (block == null) {
          throw new IllegalStateException(s"No shuffle block registered for $shuffleBlockId")
        }
        block.toManagedBuffer
      case _ =>
        throw new IllegalArgumentException(s"$blockId is not a shuffle block")
    }
  }

  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    blocks.keySet().asScala
      .filter(id => id.shuffleId == shuffleId && id.mapId == mapId)
      .toSeq
  }

  /** Release every block of `shuffleId`: free reserved memory and delete any spilled files. */
  def removeShuffle(shuffleId: Int): Unit = {
    val it = blocks.entrySet().iterator()
    var freedMemory = 0L
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey.shuffleId == shuffleId) {
        freedMemory += freeBlock(entry.getValue)
        it.remove()
      }
    }
    releaseMemory(freedMemory)
  }

  /** Free a block's underlying resource; returns the reserved memory that should be released. */
  private def freeBlock(block: StoredBlock): Long = {
    block match {
      case onDisk: OnDiskBlock =>
        if (onDisk.file.exists()) onDisk.file.delete()
        0L
      case inMemory: InMemoryBlock =>
        inMemory.reservedMemory
    }
  }

  private def releaseMemory(bytes: Long): Unit = {
    if (bytes > 0L) {
      memoryManager.releaseStorageMemory(bytes, MemoryMode.ON_HEAP)
      reservedBytes.addAndGet(-bytes)
    }
  }

  // Push-based shuffle merge is not supported: the memory manager produces no merged blocks.
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] =
    throw new UnsupportedOperationException(
      "MemoryShuffleManager does not support push-based shuffle merge")

  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta =
    throw new UnsupportedOperationException(
      "MemoryShuffleManager does not support push-based shuffle merge")

  // --- MigratableResolver: executor decommissioning / shuffle block migration ---

  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
    blocks.keySet().asScala.iterator
      .filterNot(id => shufflesToSkip.contains(id.shuffleId))
      .map(id => ShuffleBlockInfo(id.shuffleId, id.mapId))
      .toSet.toSeq
  }

  override def addShuffleToSkip(shuffleId: Int): Unit = {
    shufflesToSkip.add(shuffleId)
  }

  override def getMigrationBlocks(
      shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId
    val dataBlocks = blocks.entrySet().asScala.iterator
      .filter(e => e.getKey.shuffleId == shuffleId && e.getKey.mapId == mapId)
      .map(e => (e.getKey.asInstanceOf[BlockId], e.getValue.toManagedBuffer))
      .toList
    if (dataBlocks.isEmpty) {
      List.empty
    } else {
      // Send the data blocks first, then an empty index-block marker. When the receiver reports the
      // index block's status, the driver re-points this map's reducers to the receiving executor --
      // only after all data blocks have already arrived there.
      val indexMarker: (BlockId, ManagedBuffer) =
        (
          ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID),
          new NioManagedBuffer(ByteBuffer.allocate(0)))
      dataBlocks :+ indexMarker
    }
  }

  override def putShuffleBlockAsStream(
      blockId: BlockId,
      serializerManager: SerializerManager): StreamCallbackWithID = {
    blockId match {
      case shuffleBlockId: ShuffleBlockId =>
        // Stream a migrated partition straight to its on-disk location on this executor. Migrated
        // blocks are rescued, not hot, so disk residency is the safe, OOM-free choice.
        val file = diskLocation(shuffleBlockId)
        val channel = Channels.newChannel(new FileOutputStream(file))
        new StreamCallbackWithID {
          override def getID: String = blockId.name
          override def onData(streamId: String, buf: ByteBuffer): Unit = {
            while (buf.hasRemaining) {
              channel.write(buf)
            }
          }
          override def onComplete(streamId: String): Unit = {
            channel.close()
            putOnDisk(shuffleBlockId, file, file.length())
          }
          override def onFailure(streamId: String, cause: Throwable): Unit = {
            channel.close()
            if (file.exists()) file.delete()
          }
        }
      case indexBlockId: ShuffleIndexBlockId =>
        // The index marker carries no payload; reporting its status on completion is what makes the
        // driver point this map's reducers at this (receiving) executor.
        new StreamCallbackWithID {
          override def getID: String = blockId.name
          override def onData(streamId: String, buf: ByteBuffer): Unit = {
            buf.position(buf.limit()) // discard: marker has no data
          }
          override def onComplete(streamId: String): Unit = {
            blockManager.reportBlockStatus(
              indexBlockId,
              BlockStatus(StorageLevel.DISK_ONLY, 0L, 0L))
          }
          override def onFailure(streamId: String, cause: Throwable): Unit = {}
        }
      case _ =>
        throw new IllegalArgumentException(s"Unexpected migration block $blockId")
    }
  }

  override def stop(): Unit = {
    var freedMemory = 0L
    blocks.values().asScala.foreach(block => freedMemory += freeBlock(block))
    blocks.clear()
    releaseMemory(freedMemory)
  }
}
