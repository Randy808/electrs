# Plan: Partitioned RocksDB Architecture

## Context
Testing PRs that touch the database layer requires waiting weeks to re-sync if a corruption occurs. Partitioning the RocksDB databases lets developers validate changes against a partial dataset that syncs in days, supports independent partition recovery, and isolates compaction. This is the first step toward a future distributed architecture.

Each of the three RocksDB databases (txstore, history, cache) is split across N separate RocksDB instances by routing rows based on the first 2 bytes (4 hex chars) of the hash field in each key. N=1 is the default and is fully backward-compatible with existing on-disk data.

---

## Partition Dispatch Logic

All data keys follow the pattern `[1 byte: type prefix][32 bytes: hash][...]`.
Global/metadata keys are 1 byte long: `t` (chain tip), `F` (compaction flag), `V` (version).

```rust
fn partition_for(&self, key: &[u8]) -> usize {
    if self.partition_count == 1 || key.len() < 3 {
        return 0;  // global keys and N=1 always go to partition 0
    }
    let prefix = u16::from_be_bytes([key[1], key[2]]);
    (prefix as usize) * self.partition_count / 65536
}
```

**Path naming convention:**
- N=1: use existing paths unchanged (`txstore`, `history`, `cache`) — backward-compatible
- N>1: use numbered paths (`txstore_0`, `txstore_1`, ..., `history_0`, etc.)

---

## Critical Observations from the Migration Binary

`src/bin/db-migrate-v1-to-v2.rs` uses the Store's DB accessors and calls:
- `db.iter_scan(b"a")`, `db.iter_scan(b"C")`, `db.iter_scan(b"S")`, `db.iter_scan(b"H")` — 1-byte prefixes
- `db.write_batch(batch, flush)` — pre-built `WriteBatch` passed directly
- `db.delete_range(b"a", b"b", ...)`, `db.delete_range(b"C", b"D", ...)` — range deletes
- `lookup_confirmations(history_db, ...)` — public function in schema.rs that currently takes `&DB`

The migration always runs on N=1 databases (V1 pre-dates partitioning), so routing to partition 0 is always correct for `write_batch` in the migration context.

---

## Implementation Steps

Each step compiles and runs correctly after completion. Dead code between steps is acceptable.

---

### Step 1: Add `db_partition_count` to `Config`

**File:** `src/config.rs`

- Add `pub db_partition_count: usize` to the `Config` struct.
- Add a `clap` `Arg` for `--db-partition-count` with `default_value("1")`.
- Parse with `value_t_or_exit!(m, "db_partition_count", usize)` in `Config::from_args`.
- Add validation: `assert!(config.db_partition_count >= 1, "db-partition-count must be >= 1")`.

Nothing reads the new field yet.

**Verify:** `cargo build` passes. Running with `--db-partition-count 4` is accepted.

---

### Step 2: Implement `PartitionedDB` in `db.rs`

**File:** `src/new_index/db.rs`

Add the `PartitionedDB` struct and its full implementation. The `Store` still uses `DB` — this is purely additive.

```rust
pub struct PartitionedDB {
    partitions: Vec<DB>,
    partition_count: usize,
}
```

#### Constructor: `PartitionedDB::open`

```rust
pub fn open(base_path: &Path, name: &str, config: &Config, verify_compat: bool) -> PartitionedDB {
    let n = config.db_partition_count;
    let partitions: Vec<DB> = if n == 1 {
        vec![DB::open(&base_path.join(name), config, verify_compat)]
    } else {
        (0..n)
            .map(|i| DB::open(&base_path.join(format!("{}_{}", name, i)), config, verify_compat))
            .collect()
    };
    let pdb = PartitionedDB { partitions, partition_count: n };
    // Write or verify partition metadata in each partition DB
    for (i, db) in pdb.partitions.iter().enumerate() {
        let encoded = bincode::serialize_little(&(n as u64, i as u64)).unwrap();
        match db.get(b"partition_config") {
            None => db.put(b"partition_config", &encoded),
            Some(existing) if existing != encoded => panic!(
                "Partition config mismatch at {}[{}]: \
                 expected (count={}, index={}) but found existing metadata. \
                 Cannot open a database with a different partition count.",
                name, i, n, i
            ),
            Some(_) => {}
        }
    }
    pdb
}
```

#### Private helper

```rust
fn partition_for(&self, key: &[u8]) -> usize {
    if self.partition_count == 1 || key.len() < 3 {
        return 0;
    }
    let prefix = u16::from_be_bytes([key[1], key[2]]);
    (prefix as usize) * self.partition_count / 65536
}
```

#### Public API (mirroring `DB`)

**Point operations** — dispatch via `partition_for`:
```rust
pub fn get(&self, key: &[u8]) -> Option<Bytes> {
    self.partitions[self.partition_for(key)].get(key)
}
pub fn put(&self, key: &[u8], value: &[u8]) {
    self.partitions[self.partition_for(key)].put(key, value)
}
pub fn put_sync(&self, key: &[u8], value: &[u8]) {
    self.partitions[self.partition_for(key)].put_sync(key, value)
}
```

**Batch writes** — split rows by partition:
```rust
pub fn write_rows(&self, rows: Vec<DBRow>, flush: DBFlush) {
    if self.partition_count == 1 {
        return self.partitions[0].write_rows(rows, flush);
    }
    let mut buckets: Vec<Vec<DBRow>> = (0..self.partition_count).map(|_| vec![]).collect();
    for row in rows {
        buckets[self.partition_for(&row.key)].push(row);
    }
    for (p, bucket) in buckets.into_iter().enumerate() {
        if !bucket.is_empty() {
            self.partitions[p].write_rows(bucket, flush);
        }
    }
}
// delete_rows: same bucketing pattern as write_rows
```

**`write_batch`** — compat shim for the migration binary (always N=1 context):
```rust
pub fn write_batch(&self, batch: rocksdb::WriteBatch, flush: DBFlush) {
    assert_eq!(self.partition_count, 1,
        "write_batch is not supported with partition_count > 1");
    self.partitions[0].write_batch(batch, flush)
}
```

**`delete_range`** — apply to all partitions (safe as a no-op for partitions with no rows in range):
```rust
pub fn delete_range<K: AsRef<[u8]>>(&self, from: K, to: K, flush: DBFlush) {
    for db in &self.partitions {
        db.delete_range(from.as_ref(), to.as_ref(), flush);
    }
}
```

**`iter_scan`** — returns `Box<dyn Iterator<Item=DBRow>>`:
- Prefix ≥ 3 bytes: dispatch to one partition (performance-critical history scans always have a 33-byte prefix).
- Prefix < 3 bytes: collect from all partitions. This covers startup scans (`D`/`B` prefixes) and migration binary usage (`a`/`C`/`S`/`H` prefixes).

```rust
pub fn iter_scan(&self, prefix: &[u8]) -> Box<dyn Iterator<Item = DBRow> + '_> {
    if self.partition_count == 1 || prefix.len() >= 3 {
        let p = self.partition_for(prefix);
        return Box::new(self.partitions[p].iter_scan(prefix));
    }
    // Fan out across all partitions. Callers of short-prefix scans all collect
    // immediately (.collect() or itertools .chunks()), so eagerness is fine.
    let rows: Vec<DBRow> = self.partitions.iter()
        .flat_map(|db| db.iter_scan(prefix))
        .collect();
    Box::new(rows.into_iter())
}
```

**`iter_scan_from` / `iter_scan_reverse`** — always single-partition (callers always use a 33-byte prefix consisting of 1 type byte + 32-byte hash); return concrete typed iterators unchanged:
```rust
pub fn iter_scan_from(&self, prefix: &[u8], start_at: &[u8]) -> ScanIterator<'_> {
    self.partitions[self.partition_for(prefix)].iter_scan_from(prefix, start_at)
}
pub fn iter_scan_reverse(&self, prefix: &[u8], prefix_max: &[u8]) -> ReverseScanIterator<'_> {
    self.partitions[self.partition_for(prefix)].iter_scan_reverse(prefix, prefix_max)
}
```

**`multi_get`** — group keys by partition, multi_get each, reassemble in original order:
```rust
pub fn multi_get<K, I>(&self, keys: I) -> Vec<Result<Option<Vec<u8>>, rocksdb::Error>>
where
    K: AsRef<[u8]>,
    I: IntoIterator<Item = K>,
{
    if self.partition_count == 1 {
        return self.partitions[0].multi_get(keys);
    }
    let keys: Vec<K> = keys.into_iter().collect();
    let mut per_partition: Vec<Vec<(usize, &[u8])>> =
        (0..self.partition_count).map(|_| vec![]).collect();
    for (idx, k) in keys.iter().enumerate() {
        per_partition[self.partition_for(k.as_ref())].push((idx, k.as_ref()));
    }
    let mut results: Vec<Option<Result<Option<Vec<u8>>, rocksdb::Error>>> =
        (0..keys.len()).map(|_| None).collect();
    for (p, group) in per_partition.into_iter().enumerate() {
        if group.is_empty() { continue; }
        let orig_indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
        let group_keys: Vec<&[u8]> = group.iter().map(|(_, k)| *k).collect();
        for (res, orig_idx) in self.partitions[p].multi_get(group_keys)
            .into_iter().zip(orig_indices)
        {
            results[orig_idx] = Some(res);
        }
    }
    results.into_iter().map(|r| r.unwrap()).collect()
}
```

**Maintenance** — apply to all partitions:
```rust
pub fn full_compaction(&self) { for p in &self.partitions { p.full_compaction(); } }
pub fn enable_auto_compaction(&self) { for p in &self.partitions { p.enable_auto_compaction(); } }
pub fn flush(&self) { for p in &self.partitions { p.flush(); } }
pub fn start_stats_exporter(&self, db_metrics: Arc<RocksDbMetrics>, db_name: &str) {
    for (i, p) in self.partitions.iter().enumerate() {
        let label = if self.partition_count == 1 { db_name.to_string() }
                    else { format!("{}_{}", db_name, i) };
        p.start_stats_exporter(Arc::clone(&db_metrics), &label);
    }
}
```

**Verify:** `cargo build` passes. `Store` still uses `DB`. No behavior change.

---

### Step 3: Wire `PartitionedDB` into `Store`

**File:** `src/new_index/schema.rs`

Replace the three `DB` fields with `PartitionedDB`:

```rust
pub struct Store {
    txstore_db: PartitionedDB,
    history_db: PartitionedDB,
    cache_db: PartitionedDB,
    ...
}
```

Update `Store::open` to call `PartitionedDB::open`:
```rust
let path = config.db_path.join("newindex");
let txstore_db = PartitionedDB::open(&path, "txstore", config, verify_compat);
let history_db = PartitionedDB::open(&path, "history", config, verify_compat);
let cache_db   = PartitionedDB::open(&path, "cache",   config, verify_compat);
```

Update accessor return types:
```rust
pub fn txstore_db(&self) -> &PartitionedDB { &self.txstore_db }
pub fn history_db(&self) -> &PartitionedDB { &self.history_db }
pub fn cache_db(&self)   -> &PartitionedDB { &self.cache_db   }
```

Update internal/public functions that currently take `&DB`:
- `fn load_blockhashes(db: &DB, ...)` → `db: &PartitionedDB`
- `fn load_blockheaders(db: &DB)` → `db: &PartitionedDB`
- `fn lookup_txos(txstore_db: &DB, ...)` → `txstore_db: &PartitionedDB`
- `fn lookup_txo(txstore_db: &DB, ...)` → `txstore_db: &PartitionedDB`
- `pub fn lookup_confirmations(history_db: &DB, ...)` → `history_db: &PartitionedDB`
  (public because the migration binary imports it directly)
- `fn start_auto_compactions(db: &DB)` on `Indexer` → `db: &PartitionedDB`

**`ChainQuery::history_iter_scan` return type is unchanged** because it calls `iter_scan_from` (not `iter_scan`), and `PartitionedDB::iter_scan_from` returns `ScanIterator<'_>` just like `DB::iter_scan_from` does.

**`ChainQuery` address search** calls `iter_scan` with a short prefix (`b"a" + prefix_str`) and uses `.map(...).collect()` on the result — `Box<dyn Iterator<Item=DBRow>>` is transparent here.

**Verify:** `cargo build` passes. `cargo test` — all integration tests pass with N=1 (default). Run `cargo build --bin db-migrate-v1-to-v2` to confirm migration binary compiles.

---

### Step 4: Update re-exports in `mod.rs`

**File:** `src/new_index/mod.rs`

```rust
pub use self::db::{DBRow, PartitionedDB, DB};
```

**Verify:** `cargo build` passes for all targets.

---

## Files Changed Summary

| Step | File | Change |
|------|------|--------|
| 1 | `src/config.rs` | Add `db_partition_count` field + `--db-partition-count` CLI arg |
| 2 | `src/new_index/db.rs` | Add `PartitionedDB` struct with full implementation |
| 3 | `src/new_index/schema.rs` | Replace `DB` with `PartitionedDB` in `Store`; update internal/public helper signatures |
| 4 | `src/new_index/mod.rs` | Re-export `PartitionedDB` |

---

## Verification

```bash
# After Step 3 — N=1 default, identical behavior to today
cargo test

# Smoke test N=4
cargo run --release --bin electrs -- --db-partition-count 4 --daemon-dir ~/.bitcoin -vvvv

# Mismatch detection: start with N=4, then restart with N=1 → expect panic with clear message
```

---

## Design Decisions

**Why global keys go to partition 0:** `key.len() < 3` → `partition_for` returns 0. The chain tip (`t`), compaction flag (`F`), and version (`V`) each have a single well-known location. The version key is also written per-partition by `DB::open` → `verify_compatibility`; the migration binary's explicit V-check only touches partition 0, which is correct for its N=1 context.

**Why `iter_scan_from` / `iter_scan_reverse` keep their concrete return types:** Both are always called with a 33-byte prefix (1 type byte + 32-byte hash), so they're always single-partition. `ChainQuery::history_iter_scan` exposes `ScanIterator` in its return type and must remain unchanged.

**Why `write_batch` asserts `partition_count == 1`:** It exists only as a compat shim for the migration binary, which always operates on N=1 data. Using it with N>1 would silently misplace rows in partition 0.

**Why `delete_range` fans out to all partitions:** Applying a range delete to each partition is always safe — if no rows match the range in a given partition, it's a no-op. This handles the migration binary correctly and is generically correct for any future use.
