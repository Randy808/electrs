I have everything I need. Here's the plan:

---

## Implementation Plan: `--db-partition-range`

### Background

The current `PartitionedDB` opens **all** `db_partition_count` partitions. The goal is to allow a node to maintain only a subset of partitions (e.g. `0-499` out of 1000), discard writes for out-of-range partitions during indexing, and return a clear HTTP error for API queries that hash to an unmanaged partition.

---

### Step 1 — Add `db_partition_range` to `Config` (purely additive)

**`src/config.rs`**

1. Add field to `Config` struct:
   ```rust
   /// If `Some((lo, hi))`, only partitions `lo..=hi` (0-indexed) are maintained.
   /// `None` means all partitions are maintained (default).
   pub db_partition_range: Option<(usize, usize)>,
   ```

2. Add a CLI arg `--db-partition-range` accepting `"lo-hi"` (e.g. `"0-499"`):
   ```
   --db-partition-range 0-499
   ```

3. Parse it in `from_args()`:
   ```rust
   db_partition_range: m.value_of("db_partition_range").map(|s| {
       let (lo, hi) = s.split_once('-').expect("invalid --db-partition-range, use 'lo-hi'");
       (lo.parse::<usize>().expect("..."), hi.parse::<usize>().expect("..."))
   }),
   ```

4. Add post-construction validation (after the existing `db_partition_count >= 1` assert):
   ```rust
   if let Some((lo, hi)) = config.db_partition_range {
       assert!(lo <= hi, "db-partition-range: start must be <= end");
       assert!(hi < config.db_partition_count,
           "db-partition-range: end ({}) must be < db-partition-count ({})", hi, config.db_partition_count);
   }
   ```

5. Add helper method on `Config`:
   ```rust
   /// Returns true if `hash_bytes` (the raw 32-byte hash, e.g. txid or scripthash)
   /// maps to a partition that this node maintains.
   pub fn is_hash_in_active_partition(&self, hash_bytes: &[u8]) -> bool {
       let Some((lo, hi)) = self.db_partition_range else { return true; };
       if self.db_partition_count == 1 || hash_bytes.len() < 2 { return true; }
       let prefix = u16::from_be_bytes([hash_bytes[0], hash_bytes[1]]);
       let idx = (prefix as usize) * self.db_partition_count / 65536;
       idx >= lo && idx <= hi
   }
   ```

**Build stays green** — the new field is `Option` with `None` default; all existing callsites are unchanged.

---

### Step 2 — Only open active partitions in `PartitionedDB` (internal refactor)

**`src/new_index/db.rs`**

1. Change storage from `Vec<DB>` to `HashMap<usize, DB>`:
   ```rust
   pub struct PartitionedDB {
       active_partitions: HashMap<usize, DB>,  // keyed by logical partition index
       partition_count: usize,
       active_range: Option<(usize, usize)>,   // mirrors Config::db_partition_range
   }
   ```

2. In `PartitionedDB::open`, only open the partitions in range:
   ```rust
   let (lo, hi) = config.db_partition_range.unwrap_or((0, n - 1));
   let active_partitions: HashMap<usize, DB> = if n == 1 {
       [(0, DB::open(&base_path.join(name), config, verify_compat))].into()
   } else {
       (lo..=hi)
           .map(|i| (i, DB::open(&base_path.join(format!("{}_{}", name, i)), config, verify_compat)))
           .collect()
   };
   ```

3. Update the `partition_config` metadata loop to only iterate `active_partitions`.

4. Add a private helper:
   ```rust
   fn is_partition_active(&self, idx: usize) -> bool {
       self.active_partitions.contains_key(&idx)
   }
   ```

5. Update every method:

   | Method | Inactive partition behaviour |
   |--------|------------------------------|
   | `get` | return `None` |
   | `put` / `put_sync` | silently skip |
   | `write_rows` / `delete_rows` | filter out rows for inactive partitions before grouping |
   | `iter_scan` (fan-out path) | iterate only `active_partitions.values()` |
   | `iter_scan_from` / `iter_scan_reverse` | `assert!(active)` — REST layer prevents reaching this |
   | `delete_range` | only apply to active partitions |
   | `flush` / `full_compaction` / `enable_auto_compaction` / `start_stats_exporter` | iterate active partitions only |
   | `multi_get` | return `Ok(None)` for keys whose partition is inactive |
   | `write_batch` / `raw_iterator` | existing N==1 assert still holds |

**Build stays green** — public method signatures are unchanged; `HashMap` needs `use std::collections::HashMap`.

---

### Step 3 — Guard API routes with a partition range check (additive)

**`src/rest.rs`**

Add a small `HttpError` constructor for this case:
```rust
impl HttpError {
    fn partition_not_supported() -> Self {
        HttpError(
            StatusCode::BAD_REQUEST,
            "Query falls outside the maintained partition range".to_string(),
        )
    }
}
```

Add the check after hash/scripthash parsing in every relevant match arm:

- **txid routes** (`/tx/:txid`, `/tx/:txid/hex`, `/tx/:txid/status`, `/tx/:txid/merkle-proof`, etc.):
  ```rust
  let hash = Txid::from_str(hash)?;
  if !config.is_hash_in_active_partition(hash.as_byte_array()) {
      return Err(HttpError::partition_not_supported());
  }
  ```

- **address/scripthash routes** (`/address/:addr`, `/scripthash/:hash`, and their sub-routes `/txs`, `/utxo`):
  ```rust
  let script_hash = to_scripthash(script_type, script_str, config.network_type)?;
  if !config.is_hash_in_active_partition(&script_hash) {
      return Err(HttpError::partition_not_supported());
  }
  ```

Block routes do not need this check (blocks are indexed by block hash, not partitioned by script/tx hash).

**Build stays green** — purely additive guards on existing routes.

---

### Summary

| Step | Files touched | Maintains build |
|------|--------------|-----------------|
| 1 | `src/config.rs` | Yes — additive field + validation |
| 2 | `src/new_index/db.rs` | Yes — internal refactor, same public API |
| 3 | `src/rest.rs` | Yes — additive error guards |

Let me know when you want me to start implementing, and I'll begin with Step 1. You can run `cargo build` after each step to verify.