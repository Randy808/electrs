use crate::metrics::{Metrics, MetricOpts, GaugeVec};

#[derive(Debug)]
pub struct RocksDbMetrics {
    // Memory table metrics
    pub num_immutable_mem_table: GaugeVec,
    pub mem_table_flush_pending: GaugeVec,
    pub cur_size_active_mem_table: GaugeVec,
    pub cur_size_all_mem_tables: GaugeVec,
    pub size_all_mem_tables: GaugeVec,
    pub num_entries_active_mem_table: GaugeVec,
    pub num_entries_imm_mem_tables: GaugeVec,
    pub num_deletes_active_mem_table: GaugeVec,
    pub num_deletes_imm_mem_tables: GaugeVec,

    // Compaction metrics
    pub compaction_pending: GaugeVec,
    pub estimate_pending_compaction_bytes: GaugeVec,
    pub num_running_compactions: GaugeVec,
    pub num_running_flushes: GaugeVec,

    // Error metrics
    pub background_errors: GaugeVec,

    // Key and data size estimates
    pub estimate_num_keys: GaugeVec,
    pub estimate_live_data_size: GaugeVec,
    pub estimate_oldest_key_time: GaugeVec,

    // Table reader memory
    pub estimate_table_readers_mem: GaugeVec,

    // File and SST metrics
    pub is_file_deletions_enabled: GaugeVec,
    pub total_sst_files_size: GaugeVec,
    pub live_sst_files_size: GaugeVec,
    pub min_obsolete_sst_number_to_keep: GaugeVec,

    // Snapshot metrics
    pub num_snapshots: GaugeVec,
    pub oldest_snapshot_time: GaugeVec,

    // Version metrics
    pub num_live_versions: GaugeVec,
    pub current_super_version_number: GaugeVec,

    // Log metrics
    pub min_log_number_to_keep: GaugeVec,

    // Level metrics
    pub base_level: GaugeVec,

    // Write metrics
    pub actual_delayed_write_rate: GaugeVec,
    pub is_write_stopped: GaugeVec,

    // Block cache metrics
    pub block_cache_capacity: GaugeVec,
    pub block_cache_usage: GaugeVec,
    pub block_cache_pinned_usage: GaugeVec,
}

impl RocksDbMetrics {
    pub fn new(metrics: &Metrics, namespace: &str) -> Self {
        // Define the label for context/caller
        let labels = &["context"];

        // TODO: Call '.namespace' on new metric opts
        Self {
            // Memory table metrics
            num_immutable_mem_table: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_immutable_mem_table"),
                "Number of immutable memtables"
            ), labels),
            mem_table_flush_pending: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_mem_table_flush_pending"),
                "Number of pending memtable flushes"
            ), labels),
            cur_size_active_mem_table: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_cur_size_active_mem_table_bytes"),
                "Current size of active memtable in bytes"
            ), labels),
            cur_size_all_mem_tables: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_cur_size_all_mem_tables_bytes"),
                "Current size of all memtables in bytes"
            ), labels),
            size_all_mem_tables: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_size_all_mem_tables_bytes"),
                "Size of all memtables in bytes"
            ), labels),
            num_entries_active_mem_table: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_entries_active_mem_table"),
                "Number of entries in active memtable"
            ), labels),
            num_entries_imm_mem_tables: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_entries_imm_mem_tables"),
                "Number of entries in immutable memtables"
            ), labels),
            num_deletes_active_mem_table: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_deletes_active_mem_table"),
                "Number of delete entries in active memtable"
            ), labels),
            num_deletes_imm_mem_tables: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_deletes_imm_mem_tables"),
                "Number of delete entries in immutable memtables"
            ), labels),

            // Compaction metrics
            compaction_pending: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_compaction_pending"),
                "Number of pending compactions"
            ), labels),
            estimate_pending_compaction_bytes: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_estimate_pending_compaction_bytes"),
                "Estimated bytes pending compaction"
            ), labels),
            num_running_compactions: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_running_compactions"),
                "Number of currently running compactions"
            ), labels),
            num_running_flushes: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_running_flushes"),
                "Number of currently running flushes"
            ), labels),

            // Error metrics
            background_errors: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_background_errors_total"),
                "Total number of background errors"
            ), labels),

            // Key and data size estimates
            estimate_num_keys: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_estimate_num_keys"),
                "Estimated number of keys"
            ), labels),
            estimate_live_data_size: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_estimate_live_data_size_bytes"),
                "Estimated live data size in bytes"
            ), labels),
            estimate_oldest_key_time: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_estimate_oldest_key_time_seconds"),
                "Estimated oldest key timestamp"
            ), labels),

            // Table reader memory
            estimate_table_readers_mem: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_estimate_table_readers_mem_bytes"),
                "Estimated memory used by table readers in bytes"
            ), labels),

            // File and SST metrics
            is_file_deletions_enabled: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_is_file_deletions_enabled"),
                "Whether file deletions are enabled (0 or 1)"
            ), labels),
            total_sst_files_size: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_total_sst_files_size_bytes"),
                "Total size of all SST files in bytes"
            ), labels),
            live_sst_files_size: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_live_sst_files_size_bytes"),
                "Size of live SST files in bytes"
            ), labels),
            min_obsolete_sst_number_to_keep: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_min_obsolete_sst_number_to_keep"),
                "Minimum obsolete SST number to keep"
            ), labels),

            // Snapshot metrics
            num_snapshots: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_snapshots"),
                "Number of snapshots"
            ), labels),
            oldest_snapshot_time: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_oldest_snapshot_time_seconds"),
                "Oldest snapshot timestamp"
            ), labels),

            // Version metrics
            num_live_versions: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_num_live_versions"),
                "Number of live versions"
            ), labels),
            current_super_version_number: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_current_super_version_number"),
                "Current super version number"
            ), labels),

            // Log metrics
            min_log_number_to_keep: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_min_log_number_to_keep"),
                "Minimum log number to keep"
            ), labels),

            // Level metrics
            base_level: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_base_level"),
                "Base level for compaction"
            ), labels),

            // Write metrics
            actual_delayed_write_rate: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_actual_delayed_write_rate"),
                "Actual delayed write rate"
            ), labels),
            is_write_stopped: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_is_write_stopped"),
                "Whether writes are stopped (0 or 1)"
            ), labels),

            // Block cache metrics
            block_cache_capacity: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_block_cache_capacity_bytes"),
                "Block cache capacity in bytes"
            ), labels),
            block_cache_usage: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_block_cache_usage_bytes"),
                "Block cache usage in bytes"
            ), labels),
            block_cache_pinned_usage: metrics.gauge_vec(MetricOpts::new(
                format!("{namespace}_rocksdb_block_cache_pinned_usage_bytes"),
                "Block cache pinned usage in bytes"
            ), labels),
        }
    }
}