use prometheus::GaugeVec;
use rocksdb;

use std::path::Path;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::new_index::db_metrics::RocksDbMetrics;
use crate::util::{bincode, Bytes};

static DB_VERSION: u32 = 1;

#[derive(Debug, Eq, PartialEq)]
pub struct DBRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct ScanIterator<'a> {
    prefix: Vec<u8>,
    iter: rocksdb::DBIterator<'a>,
    done: bool,
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = DBRow;

    fn next(&mut self) -> Option<DBRow> {
        if self.done {
            return None;
        }
        let (key, value) = self.iter.next()?.expect("valid iterator");
        if !key.starts_with(&self.prefix) {
            self.done = true;
            return None;
        }
        Some(DBRow {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }
}

pub struct ReverseScanIterator<'a> {
    prefix: Vec<u8>,
    iter: rocksdb::DBRawIterator<'a>,
    done: bool,
}

impl<'a> Iterator for ReverseScanIterator<'a> {
    type Item = DBRow;

    fn next(&mut self) -> Option<DBRow> {
        if self.done || !self.iter.valid() {
            return None;
        }

        let key = self.iter.key().unwrap();
        if !key.starts_with(&self.prefix) {
            self.done = true;
            return None;
        }

        let row = DBRow {
            key: key.into(),
            value: self.iter.value().unwrap().into(),
        };

        self.iter.prev();

        Some(row)
    }
}

#[derive(Debug)]
pub struct DB {
    db: rocksdb::DB,
    db_stats: RocksDbMetrics
}

#[derive(Copy, Clone, Debug)]
pub enum DBFlush {
    Disable,
    Enable,
}

impl DB {
    pub fn open(path: &Path, config: &Config, metrics: &Metrics, namespace: &str) -> DB {
        debug!("opening DB at {:?}", path);
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.set_max_open_files(100_000); // TODO: make sure to `ulimit -n` this process correctly
        db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        db_opts.set_target_file_size_base(1_073_741_824);
        db_opts.set_write_buffer_size(256 << 20);
        db_opts.set_disable_auto_compactions(!config.initial_sync_compaction); // for initial bulk load

        // db_opts.set_advise_random_on_open(???);
        db_opts.set_compaction_readahead_size(1 << 20);
        db_opts.increase_parallelism(2);

        // let mut block_opts = rocksdb::BlockBasedOptions::default();
        // block_opts.set_block_size(???);

        let db = DB {
            db: rocksdb::DB::open(&db_opts, path).expect("failed to open RocksDB"),
            db_stats: RocksDbMetrics::new(&metrics, &namespace)
        };
        db.verify_compatibility(config);
        db
    }

    pub fn full_compaction(&self) {
        // TODO: make sure this doesn't fail silently
        debug!("starting full compaction on {:?}", self.db);
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        debug!("finished full compaction on {:?}", self.db);
    }

    pub fn enable_auto_compaction(&self) {
        let opts = [("disable_auto_compactions", "false")];
        self.db.set_options(&opts).unwrap();
    }

    pub fn raw_iterator(&self) -> rocksdb::DBRawIterator {
        self.db.raw_iterator()
    }

    pub fn iter_scan(&self, prefix: &[u8]) -> ScanIterator {
        ScanIterator {
            prefix: prefix.to_vec(),
            iter: self.db.prefix_iterator(prefix),
            done: false,
        }
    }

    pub fn iter_scan_from(&self, prefix: &[u8], start_at: &[u8]) -> ScanIterator {
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            start_at,
            rocksdb::Direction::Forward,
        ));
        ScanIterator {
            prefix: prefix.to_vec(),
            iter,
            done: false,
        }
    }

    pub fn iter_scan_reverse(&self, prefix: &[u8], prefix_max: &[u8]) -> ReverseScanIterator {
        let mut iter = self.db.raw_iterator();
        iter.seek_for_prev(prefix_max);

        ReverseScanIterator {
            prefix: prefix.to_vec(),
            iter,
            done: false,
        }
    }

    pub fn write(&self, mut rows: Vec<DBRow>, flush: DBFlush) {
        log::trace!(
            "writing {} rows to {:?}, flush={:?}",
            rows.len(),
            self.db,
            flush
        );
        rows.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let mut batch = rocksdb::WriteBatch::default();
        for row in rows {
            batch.put(&row.key, &row.value);
        }
        let do_flush = match flush {
            DBFlush::Enable => true,
            DBFlush::Disable => false,
        };
        let mut opts = rocksdb::WriteOptions::new();
        opts.set_sync(do_flush);
        opts.disable_wal(!do_flush);
        self.db.write_opt(batch, &opts).unwrap();
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).unwrap();
    }

    pub fn put_sync(&self, key: &[u8], value: &[u8]) {
        let mut opts = rocksdb::WriteOptions::new();
        opts.set_sync(true);
        self.db.put_opt(key, value, &opts).unwrap();
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.db.get(key).unwrap().map(|v| v.to_vec())
    }

    pub fn multi_get<K, I>(&self, keys: I) -> Vec<Result<Option<Vec<u8>>, rocksdb::Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        self.db.multi_get(keys)
    }

    fn verify_compatibility(&self, config: &Config) {
        let mut compatibility_bytes = bincode::serialize_little(&DB_VERSION).unwrap();

        if config.light_mode {
            // append a byte to indicate light_mode is enabled.
            // we're not letting bincode serialize this so that the compatiblity bytes won't change
            // (and require a reindex) when light_mode is disabled. this should be chagned the next
            // time we bump DB_VERSION and require a re-index anyway.
            compatibility_bytes.push(1);
        }

        match self.get(b"V") {
            None => self.put(b"V", &compatibility_bytes),
            Some(ref x) if x != &compatibility_bytes => {
                panic!("Incompatible database found. Please reindex.")
            }
            Some(_) => (),
        }
    }

    // Phil Ref
    pub fn print_stats(self) {
         // Example: Get the number of entries
        if let Some(value) = self.db.property_value("rocksdb.estimate-num-keys").unwrap() {
            println!("Estimated number of keys: {}", value);
        }

        // Example: Get the size of all SST files
        if let Some(value) = self.db.property_value("rocksdb.total-sst-files-size").unwrap() {
            println!("Total SST file size: {} bytes", value);
        }

        // Example: Get statistics (if enabled)
        if let Some(stats) = self.db.property_value("rocksdb.stats").unwrap() {
            println!("RocksDB Stats:\n{}", stats);
        }

        // rocksdb.compression-ratio-at-level0 rocksdb.compression-ratio-at-level1 rocksdb.compression-ratio-at-level2
        if let Some(asdf) = self.db.property_value("rocksdb.compression-ratio-at-level0").unwrap() {
            println!("RocksDB CompressionRatio level0:\n{}", asdf);
        }


        // rocksdb.compression-ratio-at-level0 rocksdb.compression-ratio-at-level1 rocksdb.compression-ratio-at-level2
        if let Some(asdf) = self.db.property_value("rocksdb.compression-ratio-at-level1").unwrap() {
            println!("RocksDB CompressionRatio level1:\n{}", asdf);
        }

        // rocksdb.compression-ratio-at-level0 rocksdb.compression-ratio-at-level1 rocksdb.compression-ratio-at-level2
        if let Some(asdf) = self.db.property_value("rocksdb.compression-ratio-at-level2").unwrap() {
            println!("RocksDB CompressionRatio level2:\n{}", asdf);
        }

        if let Some(value) = self.db.property_int_value("rocksdb.num-running-compactions").unwrap() {
            println!("Number of running compactions: {}", value);
        }
    }

    // Updated method that takes a context string
    pub fn update_from_db(&self, context: &str) {
        // Helper closure to parse and set gauge values with context label
        let update_gauge = |gauge: &GaugeVec, property: &str| {
            if let Ok(Some(value)) = self.db.property_value(property) {
                if let Ok(v) = value.parse::<f64>() {
                    gauge.with_label_values(&[context]).set(v);
                }
            }
        };

        // Update all metrics
        update_gauge(&self.db_stats.num_immutable_mem_table, "rocksdb.num-immutable-mem-table");
        update_gauge(&self.db_stats.mem_table_flush_pending, "rocksdb.mem-table-flush-pending");
        update_gauge(&self.db_stats.compaction_pending, "rocksdb.compaction-pending");
        update_gauge(&self.db_stats.background_errors, "rocksdb.background-errors");
        update_gauge(&self.db_stats.cur_size_active_mem_table, "rocksdb.cur-size-active-mem-table");
        update_gauge(&self.db_stats.cur_size_all_mem_tables, "rocksdb.cur-size-all-mem-tables");
        update_gauge(&self.db_stats.size_all_mem_tables, "rocksdb.size-all-mem-tables");
        update_gauge(&self.db_stats.num_entries_active_mem_table, "rocksdb.num-entries-active-mem-table");
        update_gauge(&self.db_stats.num_entries_imm_mem_tables, "rocksdb.num-entries-imm-mem-tables");
        update_gauge(&self.db_stats.num_deletes_active_mem_table, "rocksdb.num-deletes-active-mem-table");
        update_gauge(&self.db_stats.num_deletes_imm_mem_tables, "rocksdb.num-deletes-imm-mem-tables");
        update_gauge(&self.db_stats.estimate_num_keys, "rocksdb.estimate-num-keys");
        update_gauge(&self.db_stats.estimate_table_readers_mem, "rocksdb.estimate-table-readers-mem");
        update_gauge(&self.db_stats.is_file_deletions_enabled, "rocksdb.is-file-deletions-enabled");
        update_gauge(&self.db_stats.num_snapshots, "rocksdb.num-snapshots");
        update_gauge(&self.db_stats.oldest_snapshot_time, "rocksdb.oldest-snapshot-time");
        update_gauge(&self.db_stats.num_live_versions, "rocksdb.num-live-versions");
        update_gauge(&self.db_stats.current_super_version_number, "rocksdb.current-super-version-number");
        update_gauge(&self.db_stats.estimate_live_data_size, "rocksdb.estimate-live-data-size");
        update_gauge(&self.db_stats.min_log_number_to_keep, "rocksdb.min-log-number-to-keep");
        update_gauge(&self.db_stats.min_obsolete_sst_number_to_keep, "rocksdb.min-obsolete-sst-number-to-keep");
        update_gauge(&self.db_stats.total_sst_files_size, "rocksdb.total-sst-files-size");
        update_gauge(&self.db_stats.live_sst_files_size, "rocksdb.live-sst-files-size");
        update_gauge(&self.db_stats.base_level, "rocksdb.base-level");
        update_gauge(&self.db_stats.estimate_pending_compaction_bytes, "rocksdb.estimate-pending-compaction-bytes");
        update_gauge(&self.db_stats.num_running_compactions, "rocksdb.num-running-compactions");
        update_gauge(&self.db_stats.num_running_flushes, "rocksdb.num-running-flushes");
        update_gauge(&self.db_stats.actual_delayed_write_rate, "rocksdb.actual-delayed-write-rate");
        update_gauge(&self.db_stats.is_write_stopped, "rocksdb.is-write-stopped");
        update_gauge(&self.db_stats.estimate_oldest_key_time, "rocksdb.estimate-oldest-key-time");
        update_gauge(&self.db_stats.block_cache_capacity, "rocksdb.block-cache-capacity");
        update_gauge(&self.db_stats.block_cache_usage, "rocksdb.block-cache-usage");
        update_gauge(&self.db_stats.block_cache_pinned_usage, "rocksdb.block-cache-pinned-usage");
    }
}
