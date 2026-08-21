# spark-memory-shuffle

An in-memory `ShuffleManager` plugin for Apache Spark.

Instead of writing map output to shuffle files on local disk, this manager keeps each map task's
partitioned output in executor memory and exchanges it directly between nodes over Spark's existing
block transfer service. Retained output is drawn from Spark's unified storage-memory pool, so when
memory runs out an individual block spills to a local file instead of the executor running out of
heap. The fast path is pure memory-to-memory; disk is the bounded fallback under pressure.

Built and tested against **Apache Spark 4.2.0** (Scala 2.13).

## Why this works as a plugin

Spark's shuffle is pluggable via `spark.shuffle.manager`. The key realization is that the reduce
side reads blocks **by `BlockId` through the block manager**, which resolves them via the manager's
`ShuffleBlockResolver` -- and a resolver may back a block with *any* `ManagedBuffer`, memory or
file. So this plugin:

- keeps Spark's classic **map/reduce decoupling** (a map task fully materializes output and
  registers a `MapStatus`; reducers pull afterwards). Fault tolerance and the read transport are
  unchanged -- only the storage medium differs;
- reuses `BlockStoreShuffleReader` and `MapOutputTracker` **verbatim** on the read path;
- reuses Spark's `ExternalSorter` **verbatim** on the write path, so map-side combine, key
  ordering, serialization/compression/encryption, checksums, and large-input spill all behave
  exactly as they do for the built-in sort shuffle. Only the sink differs: finished partitions land
  in memory (or a local file) rather than in the shuffle data/index files.

## How memory is bounded (no OOM)

Two independent limits keep the heap safe:

1. **Retention** -- each in-memory block reserves its size from the unified storage-memory pool via
   `MemoryManager.acquireStorageMemory`. If the pool cannot grant it (full even after evicting
   evictable storage), the block is written to a local file and served as a
   `FileSegmentManagedBuffer`. Reservations never exceed what the pool grants, so retained output
   cannot push the heap past the storage ceiling -- the executor spills instead of failing.
2. **Write-time** -- while a single partition is being written, its bytes accumulate in a heap
   buffer only up to `spark.shuffle.memory.perPartitionWriteBufferBytes` (default 16 MiB); beyond
   that the partition promotes to a file and streams the rest to disk. This bounds the worst-case
   spike from any one (possibly skewed) partition.

Reserved memory is released, and spilled files deleted, when the shuffle is unregistered.

**Cooperativeness note:** retained-block reservations are pinned -- the `MemoryManager` will not
evict a live shuffle block to satisfy another request (blocks are not registered with the
`MemoryStore` eviction callback). Execution memory therefore treats retained shuffle output as
unreclaimable and adapts by spilling its own state, which is safe (no OOM), just less elastic than
cached RDD blocks. This is the deliberate trade for keeping the read path a drop-in.

## Layout

| File | Role |
|------|------|
| `MemoryShuffleManager` | The `ShuffleManager` entry point: registration, writer/reader wiring. |
| `MemoryShuffleWriter` | Drives an `ExternalSorter` (combine/ordering/input-spill) into the write plugin. |
| `MemoryShuffleExecutorComponents` | In-memory `ShuffleMapOutputWriter`; per-partition write-buffer cap + disk promotion. |
| `MemoryShuffleBlockResolver` | Stores/serves blocks; storage-memory accounting and disk spill. |
| `MemoryShuffleConf` | Configuration keys. |

## Build & test

```bash
sbt compile
sbt test        # real shuffles (reduceByKey/groupByKey/sortByKey/join/chained), in-memory + forced-disk
sbt package     # produces the plugin jar
```

`spark-core` is a `provided` dependency, so the jar is thin -- drop it on the Spark classpath.

> **Offline / restricted networks:** if Maven Central is blocked, point sbt at an internal mirror
> (e.g. via `~/.sbt/repositories` with `override.build.repos = true`) so `spark-core:4.2.0`
> resolves.

## Use

```
spark.shuffle.manager                              org.apache.spark.shuffle.memory.MemoryShuffleManager
spark.shuffle.service.enabled                      false   # ESS cannot serve memory-resident blocks
spark.shuffle.memory.perPartitionWriteBufferBytes  16m     # per-partition heap write cap (optional)
```

## Executor decommissioning

The resolver implements `MigratableResolver`, so a decommissioned executor's retained shuffle
output is migrated to a peer instead of being lost. Because the in-memory format has no index file,
migration sends each per-partition block followed by an empty `ShuffleIndexBlockId` marker; the
marker's status report on the receiving side is what drives the driver to re-point that map's
reducers at the new executor (matching how `BlockManagerMasterEndpoint` handles index-block
updates). Migrated blocks are streamed straight to local disk on the receiver. Enable with the
usual `spark.storage.decommission.enabled` / `spark.storage.decommission.shuffleBlocks.enabled`.

## Operational notes

- **External Shuffle Service:** memory-resident blocks cannot be served by the ESS after an
  executor dies, so a memory shuffle deployment should disable it and accept map-stage
  recomputation on executor loss (decommissioning migrates blocks first; see above).
- **Push-based shuffle merge** is not supported (the manager produces no merged blocks); leave
  `spark.shuffle.push.enabled` off.
- **Cross-node transfer:** blocks are served through the unchanged block-transfer layer as standard
  `ManagedBuffer`s (`NioManagedBuffer` for memory, `FileSegmentManagedBuffer` for spilled), so
  remote reads use exactly the same Netty path as built-in shuffle.

## Tests

- `MemoryShuffleManagerSuite` (single JVM): correctness for reduceByKey/groupByKey/sortByKey/join/
  chained/empty in both in-memory and forced-disk modes; a volume run; a **storage-pool-exhaustion
  spill** test that verifies real memory-pressure spilling; and **migration mechanics** tests for
  `getStoredShuffles` / `getMigrationBlocks` / `putShuffleBlockAsStream`.
- `MemoryShuffleClusterSuite` (`local-cluster`, multiple executor JVMs): a **cross-process shuffle**
  test that proves node-to-node fetch, and an executor-removal continuity test. The latter is
  environment-gated -- it self-cancels where an executor cannot be fully decommissioned/removed
  (e.g. macOS, which lacks `SIGPWR`), and runs end-to-end on Linux CI.

## License

Apache License 2.0.
