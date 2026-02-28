# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Esplora's electrs backend — a Bitcoin (and Liquid/Elements) blockchain index engine and HTTP REST API, forked from romanz/electrs. It powers [blockstream.info](https://blockstream.info). The active development branch is `new-index`.

## Build & Run Commands

```bash
# Build (standard)
cargo build --release

# Build with Liquid support
cargo build --features liquid --release

# Run the server
cargo run --release --bin electrs -- -vvvv --daemon-dir ~/.bitcoin

# Run with Liquid
cargo run --features liquid --release --bin electrs -- -vvvv --network liquid --daemon-dir ~/.liquid

# Show all CLI options
cargo run --release --bin electrs -- --help
```

## Testing

Integration tests spawn real `bitcoind`/`elementsd`/`electrumd` instances. They require those binaries available on PATH (or via the crate's download mechanism).

```bash
# Run all tests
cargo test

# Run a specific test by name
cargo test test_rest_tx

# Run tests for a specific file/module
cargo test --test rest
cargo test --test electrum

# Run with Liquid feature
cargo test --features liquid

# Run with verbose output (shows node stdout)
RUST_LOG=info cargo test

# Force JSONRPC import mode (faster for small chains, skips blk file parsing)
JSONRPC_IMPORT=1 cargo test
```

## Linting & Formatting

```bash
cargo clippy
cargo fmt
cargo fmt --check
```

## Benchmarks

```bash
cargo bench --features bench
```

## Architecture

### Data Flow

1. **Daemon** (`src/daemon.rs`) — Communicates with `bitcoind`/`elementsd` via JSON-RPC, reads raw block files, fetches mempool.
2. **Indexer** (`src/new_index/schema.rs` — `Indexer`) — Two-phase indexing: phase 1 writes `txstore` (raw transactions, outputs), phase 2 writes `history` (script hash history, spending inputs).
3. **Store** (`src/new_index/schema.rs` — `Store`) — Wraps three RocksDB databases: `txstore`, `history`, `cache`.
4. **ChainQuery** (`src/new_index/schema.rs`) — Read interface over `Store` + `Daemon`.
5. **Mempool** (`src/new_index/mempool.rs`) — In-memory mempool state, synced from `Daemon`.
6. **Query** (`src/new_index/query.rs`) — High-level read API combining `ChainQuery` + `Mempool`; used by both servers.
7. **REST server** (`src/rest.rs`) — HTTP API via `hyper` + `tokio`, serves the Esplora API endpoints.
8. **Electrum RPC server** (`src/electrum/server.rs`) — TCP server implementing the Electrum protocol (v1.4).

### Database Schema

Three RocksDB databases (in `{db_path}/newindex/`):
- **`txstore`** — Block headers (`B`), txid→raw tx (`T`), txid list per block (`X`), block metadata (`M`), outputs (`O`), address prefix search (`a` — optional).
- **`history`** — Per-scripthash history rows (`H`), spending index (`S`), confirmation index (`C`).
- **`cache`** — On-demand aggregated stats (`A`) and UTXOs (`U`) per scripthash.

See `doc/schema.md` for the full key format documentation.

### Feature Flags

- **`liquid`** — Enables Elements/Liquid support (CT transactions, peg-in/out, multi-asset). Mutually exclusive path from Bitcoin in `src/chain.rs` via cfg attributes. Liquid-specific code lives in `src/elements/`.
- **`electrum-discovery`** — P2P server discovery for the Electrum network.
- **`otlp-tracing`** — OpenTelemetry tracing export.
- **`bitcoind_28_0`** — Compatibility flag for bitcoind 28.0+ API changes.
- **`bench`** — Enables benchmarks in `benches/`.

### Key Modules

| Path | Purpose |
|------|---------|
| `src/chain.rs` | Re-exports Bitcoin or Elements types depending on feature flag |
| `src/config.rs` | CLI argument parsing (via `clap`), `Config` struct |
| `src/daemon.rs` | `Daemon` — JSON-RPC client to bitcoind, with retry logic |
| `src/new_index/schema.rs` | Core index types: `Store`, `Indexer`, `ChainQuery`, history rows |
| `src/new_index/mempool.rs` | `Mempool` — live mempool state |
| `src/new_index/query.rs` | `Query` — unified read API over chain + mempool |
| `src/new_index/db.rs` | RocksDB wrapper |
| `src/new_index/fetch.rs` | Block fetching from blk files or RPC |
| `src/rest.rs` | HTTP REST API (hyper + tokio) |
| `src/electrum/server.rs` | Electrum TCP protocol server |
| `src/elements/` | Liquid/Elements-specific logic (CT, assets, pegs) |
| `electrs_macros/` | Proc-macro crate (`#[trace]` attribute for OTLP tracing) |

### Test Infrastructure

Tests in `tests/` use:
- `tests/common.rs` — `TestRunner` struct that spins up a real `bitcoind`/`elementsd` instance, a full electrs `Store`/`Indexer`/`Query`, and helper methods (`mine`, `send`, `sync`).
- `tests/rest.rs` — Integration tests for the HTTP REST API.
- `tests/electrum.rs` — Integration tests for the Electrum RPC protocol (also spins up a real `electrumd` wallet).

### Rust Toolchain

Pinned to `1.75.0` via `rust-toolchain.toml`.

### Light Mode

`--lightmode` skips storing raw transactions (`T`), block txid lists (`X`), and block metadata (`M`) in RocksDB. Those are queried from bitcoind on demand, reducing disk usage ~50% at the cost of slower lookups.
