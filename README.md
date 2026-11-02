# Python & PostgreSQL Data Pipeline Optimization Challenge — Solution Repository

**License:** [Apache License 2.0](LICENSE)

This repository contains our high-performance, fault-tolerant optimization of the **PGDM DB Manager** PostgreSQL JSONB expansion pipeline (`public.raw_data` $\rightarrow$ `public.table1..table6`) for the [Python & PostgreSQL Data Pipeline Optimization Challenge](https://www.kaggle.com/competitions/python-postgre-sql-data-pipeline-optimization-challenge).

---

## Key Highlights & Results

| Scale (`public.raw_data` Rows) | Inserted Destination Rows (`table1..table6`) | Baseline Measured Total | Optimized Measured Total | Speedup | Temp Disk Spill (Baseline $\rightarrow$ Optimized) | Relational Output Checksum |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`100,000`** | `532,416` | `8.137 s – 9.655 s` | **`2.700 s`** | **`3.58x`** | `284.00 MB` $\rightarrow$ **`0.06 MB`** | **100% Bit-for-Bit Match** |
| **`1,000,000`** | `5,321,553` | `174.937 s (2m 54.937s)` | **`28.903 s`** | **`6.05x`** | `2,739.71 MB` $\rightarrow$ **`0.06 MB` (`41,804x` less)** | **100% Bit-for-Bit Match** |
| **`10,000,000`** | `53,225,999` | `~35+ min` (projected; $>27\text{ GB}$ temp spill) | **`445.033 s (7m 25.033s)`** | **Linear Scaling (10 Chunks)** | $>27,400\text{ MB}$ $\rightarrow$ **`0.63 MB` (`43,800x` less)** | **100% Verified** |

---

## Core Architectural Optimizations

1. **Single-Pass Parallel JSONB Unpack & Staging (`public.pgdm_stage_unified`):**
   - Replaces **19 full-table scans** of `public.raw_data` and **730+ million JSONB path extractions (`->`, `->>`) per 1M rows** with **1 parallel scan** (`max_parallel_workers_per_gather = 6`) using `CROSS JOIN LATERAL jsonb_to_record(s.raw -> 'values')` + a single 15-tuple `VALUES` unpivot (`pos = 0` for base entity attributes and `pos = 1..14` for child position attributes).
   - Replaces the correlated 14-row `LEFT JOIN LATERAL (SELECT MAX(v.pos) ...)` subquery in `table1` with a flat, branch-free `GREATEST(...)` expression across `integer_column_2..15`, accelerating `public.table1` insertion by **`19.91x`** (`98.975 s` $\rightarrow$ `4.972 s` on `1M` rows).
   - Consolidates 14 separate `INSERT INTO public.table2` statements into a **single set-based `INSERT`** reading `WHERE pos > 0`, accelerating `public.table2` insertion by **`4.19x`** (`37.383 s` $\rightarrow$ `8.917 s` on `1M` rows).

2. **Direct Surrogate-Key Mapping (`pg_temp.pgdm_t1_map`), Precomputed `table4` Key & Adaptive Non-Superuser FK Guard:**
   - Captures `(raw_id, t1_id)` directly from `INSERT INTO public.table1 ... RETURNING id_column_3, id_column_1` into a compact `16-byte/row` temporary mapping table (`pg_temp.pgdm_t1_map`), eliminating all downstream joins against `public.table1` and `public.table4` when populating `table5`, `table6`, and `table2`.
   - Enforces pure in-memory `Hash Join` execution (`SET LOCAL work_mem = '256MB'`, `SET LOCAL temp_buffers = '256MB'`, `SET LOCAL enable_mergejoin = off`), reducing PostgreSQL temporary sort/hash disk spill on `1,000,000` rows from **`2,739.71 MB` (`13` files) to `0.06 MB` (`64 KiB`)**.
   - Works identically under both `SUPERUSER` accounts (`SET LOCAL session_replication_role = 'replica'`) and standard `LOGIN CREATEDB` non-superuser accounts (catching `insufficient_privilege` via PL/pgSQL and switching to batch foreign-key constraint drop/re-validation so 5.32M per-row FK triggers never fire during bulk load).

3. **Consolidated Preflight & Adaptive SSH Transport Polling:**
   - Collapses **23–29 sequential `psql` process startups** during preflight validation into **2 consolidated JSON queries** (1 against `CONTROL_DB` and 1 against `kaggle_challenge`), reducing preflight latency by **`7.16x–18.1x`** (`523.77 ms` $\rightarrow$ `73.16 ms`).
   - Replaces fixed `50 ms / 100 ms` `time.sleep()` polling loops in `services/postgres_service.py` with adaptive exponential backoff (`0.5 ms` $\rightarrow$ `5 ms`) and caches `ensure_admin_schema()` per connection.

4. **Chunked Execution & Self-Healing Checkpoint Crash Recovery (`public.pgdm_expansion_checkpoints`):**
   - Automatically partitions arbitrarily large `raw_data` scopes (`10M+` rows) into bounded `raw_id` chunks (`expansion_chunk_size = 1,000,000` by default) scanned via `Parallel Index Scan using raw_data_pkey` (`SET LOCAL min_parallel_index_scan_size = 0`).
   - Persists per-chunk completion state, hash manifests, and timing telemetry atomically inside each chunk's transaction in `public.pgdm_expansion_checkpoints`.
   - Synchronizes destination identity sequences (`setval(...)`) before and after each chunk so interrupted runs (network drops, server crashes, or pre-versioning failures) resume from the exact uncommitted chunk with **zero duplicate rows and zero sequence gaps**.

---

## Repository Structure

- [`services/postgres_service.py`](services/postgres_service.py): Core backend service containing the consolidated preflight, single-pass parallel expansion engine, chunked checkpoint recovery, and adaptive SSH channel polling.
- [`SQL files/`](SQL%20files/): Optimized standalone SQL recipes (`01_table3.sql` through `06_table2.sql`) compatible with the GUI's `Expand` dialog and standalone execution.
- [`benchmark_suite.py`](benchmark_suite.py): Reproducible headless CLI benchmark, 128-bit parallel table checksum verifier, and automated crash-recovery test suite (Scenarios B & C).
- [`tests/`](tests/): Unit test suites (`test_challenge_expansion_recipes.py`, `test_expand_ui_english.py`, `test_expansion_timing.py`, and `test_expansion_recovery.py`).
- [`BENCHMARK_AND_ARCHITECTURE.md`](BENCHMARK_AND_ARCHITECTURE.md): Detailed technical architecture, query execution plans, bottleneck breakdown, and multi-scale benchmark telemetry.
- [`generate_database.py`](generate_database.py): Official deterministic dataset generator (unmodified).

---

## Reproducible Setup & Verification Instructions

### 1. Prerequisites
- Python 3.10+ (`pip install -r requirements.txt`)
- PostgreSQL 14+ (tested on PostgreSQL 16) with `psql` in `PATH`

### 2. Generate the Deterministic Challenge Dataset
```bash
# 100,000 rows (~200 MB)
python3 generate_database.py --rows 100000 --host localhost --user postgres --replace

# 1,000,000 rows (~2.6 GB)
python3 generate_database.py --rows 1000000 --host localhost --user postgres --replace

# 10,000,000 rows (~26 GB)
python3 generate_database.py --rows 10000000 --host localhost --user postgres --replace
```

### 3. Run Unit Tests
```bash
python3 -m unittest discover -s tests -v
```

### 4. Run Headless Expansion Benchmark & Crash-Recovery Verification
```bash
# Run on local PostgreSQL instance and verify mid-expansion + post-commit crash recovery:
python3 benchmark_suite.py --host localhost --pg-user postgres --local-subprocess --verify-crash-recovery
```

### 5. Run via GUI Application
```bash
python3 main.py
```
Connect to your PostgreSQL server, select `kaggle_challenge` $\rightarrow$ `public.raw_data` $\rightarrow$ **Expand**, load `SQL files/01_table3.sql` .. `06_table2.sql`, and click **Execute**. The full `EXPANSION PERFORMANCE REPORT` dialog will open automatically upon completion.
