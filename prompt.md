Here is a proposal that I'd like you to make an implementation plan for. Each step should be small and implement a small piece of the logic in a way where the application can still be built after completing each step. I'm also okay with there being dead code in one step if it's in an effort to keep each step change small. Also please keep it to just db partitioning for now. I want to add what's in the future direction part later, but I only included here to give you context in hopes it'll help you better architect this change for adaptation:

# Proposal: Partitioned electrs Architecture

## Motivation
Our current electrs setup has operational challenges that slow development. Initial sync takes weeks, and resource usage spikes unpredictably with block size and request volume. Most critically, testing PRs that touch the database layer is painful—if a change corrupts the DB, we're restoring from backup and re-syncing for weeks. This discourages improvements to the very code paths that most need optimization.

## Approach
Partition the database by hash prefix. Using the first 4 hex characters of stored data (65,536 possible prefixes), we divide the keyspace across N separate RocksDB instances. A configuration value determines the partition count, and each partition covers an equal range of prefixes. For example, with 5 partitions, txstore_1 holds prefixes 0000-3332, txstore_2 holds 3333-6665, and so on.
Each database includes metadata identifying its partition configuration, ensuring mismatches are caught early.

## Immediate Benefits
Faster testing: Validate DB-touching PRs against a partial dataset that syncs in days, not weeks
Independent recovery: A corrupted partition can be restored without re-syncing everything
Isolated compaction: Partitions compact independently, reducing resource spikes

## Future Direction
This is the first step toward a fully distributed electrs architecture. Later phases would split partitions across multiple electrs instances, then across separate machines with a routing layer. The partition boundary logic remains consistent throughout, so data can be moved between setups without re-indexing.