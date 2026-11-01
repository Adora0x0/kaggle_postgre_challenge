# Technical Architecture, Bottleneck Analysis & Multi-Scale Benchmark Report

## 1. Executive Summary

This document presents the engineering analysis, architectural redesign, failure-recovery guarantees, and empirical benchmark results for our solution to the **Python & PostgreSQL Data Pipeline Optimization Challenge**.

Our optimization targets the full end-to-end `Expand` workflow of the **PGDM DB Manager** (`services/postgres_service.py` and `SQL files/01_table3.sql..06_table2.sql`), which transforms semi-structured 64-key JSONB documents in `public.raw_data` into a normalized 6-table relational schema (`public.table3`, `public.table4`, `public.table1`, `public.table5`, `public.table6`, and `public.table2`) while maintaining version lineage, dependency manifests, and exact row counters.

### Summary of Multi-Scale Benchmark Results

| Scale (`public.raw_data`) | Inserted Rows (`table1..table6`) | Baseline Measured Total | Optimized Measured Total | Speedup | Temp Disk Spill (Baseline $\rightarrow$ Optimized) | Output Equivalence |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`100,000`** | `532,416` | `8.137 s – 9.655 s` | **`2.657 s`** | **`3.63x`** | `284.00 MB` (`21` files) $\rightarrow$ **`0.06 MB`** | **100% Bit-for-Bit Match** |
| **`1,000,000`** | `5,321,553` | `174.937 s (2m 54.937s)` | **`28.903 s`** | **`6.05x`** | `2,739.71 MB` (`13` files) $\rightarrow$ **`0.06 MB` (`41,804x` less)** | **100% Bit-for-Bit Match** |
| **`10,000,000`** | `53,225,999` | `~35+ min` ($>27\text{ GB}$ temp spill) | **`445.033 s (7m 25.033s)`** | **Linear Scaling (10 Chunks)** | $>27,400\text{ MB}$ $\rightarrow$ **`0.63 MB` (`43,800x` less)** | **100% Verified** |

---

## 2. Bottlenecks Identified in the Baseline Pipeline

Through fine-grained server-side `clock_timestamp()` telemetry, `EXPLAIN (ANALYZE, BUFFERS)`, and `pg_stat_database` profiling on `100,000` and `1,000,000` rows, we identified **6 primary bottlenecks** across the SQL recipes, PostgreSQL execution plans, and Python/SSH orchestration layer:

### Bottleneck 1: `19x` Redundant Full-Table Scans & `730M+` JSONB Traversals per 1M Rows
- The baseline pipeline executes **19 separate `SELECT ... FROM public.raw_data` scans**:
  - `1` scan to build `pgdm_cross_table_hash_manifest`
  - `1` scan each for `table3`, `table4`, `table1`, `table5`, and `table6`
  - **`14` separate full-table scans** in `06_table2.sql` (one `INSERT` statement per position `1..14`)
- Across these 19 scans, PostgreSQL decompresses the TOASTed/varlena `raw` JSONB column 19 times and executes **over 730 million `s.raw -> 'values' ->> '...'` key lookups per 1,000,000 rows**.

### Bottleneck 2: Correlated 14-Row `LATERAL` Subquery in `03_table1.sql` (`98.975 s` on 1M Rows — `56.6%` of Total Time)
- In the baseline `03_table1.sql`, computing `integer_column_1` (`max_position`) runs a correlated `LEFT JOIN LATERAL (SELECT MAX(v.pos)::smallint FROM (VALUES (1, ...), ..., (14, ...)) v(pos, raw_pos) WHERE NULLIF(BTRIM(v.raw_pos), '')::smallint = v.pos)` for every single row, plus a 5-column non-indexed join against `public.table3` where JSONB strings are re-extracted inside the `ON` clause.
- On `1,000,000` rows, `Insert into public.table1` alone consumed **`98.975 s`** (`56.6%` of the entire `174.937 s` baseline runtime).

### Bottleneck 3: `14` Separate Statements & Repeated `table1` Joins in `06_table2.sql` (`37.383 s` on 1M Rows)
- `06_table2.sql` executes 14 separate `INSERT INTO public.table2 ... SELECT FROM public.raw_data s JOIN public.table1 p ON p.id_column_3 = s.raw_id` statements.
- Each of the 14 statements scans the entire `raw_data` table, joins against `public.table1` (`1M` rows), and updates `pgdm_table_row_counts` 14 times.

### Bottleneck 4: External Sort / MergeJoin Disk Spill (`2.74 GB` Temp Disk I/O on 1M Rows)
- With default `work_mem` (`4MB`), joining `1,000,000` rows of `raw_data` against `1,000,000` rows of `public.table1` in `table5`, `table6`, and `table2` triggers external disk-based `Sort` + `Merge Join` or multi-batch `Hash Join`, writing **`13` temporary files totaling `2,739.71 MB` (`2.55 GiB`)** on `1M` rows (`284 MB` on `100k` rows).

### Bottleneck 5: Redundant Per-Row Foreign Key Trigger Checks & Uncached Sequences
- Every row inserted into `table1` (`2` FKs), `table5` (`1` FK), `table6` (`2` FKs), and `table2` (`1` FK) fires internal `RI_FKey_check_ins` trigger queries (`6,321,553` index lookups per 1M source rows) even though foreign keys are already guaranteed by the `INSERT ... SELECT ... JOIN` queries.
- Default sequence `CACHE 1` forces per-row sequence state contention across `5.32M` `IDENTITY` allocations per 1M rows.

### Bottleneck 6: `23–29` Sequential `psql` Process Invocations & `50ms/100ms` SSH Polling Sleep
- Before executing a single row of expansion SQL, `execute_cross_table_expansion` spawned **23 to 29 sequential remote `psql` processes** (checking `ensure_admin_schema`, table existence, column definitions, function safety, row counters, triggers/rules/RLS/inheritance for each of the 6 destination tables).
- In `run_remote_command` and `iter_remote_command_lines`, fixed `time.sleep(0.05)` and `time.sleep(0.1)` polling intervals added `50–100 ms` of artificial latency per remote command.

---

## 3. Optimization Strategy & Architectural Redesign

```
+-----------------------------------------------------------------------------------+
|                        1. CONSOLIDATED PREFLIGHT (2 Queries)                      |
|  - Control DB (1 JSON query): source_versions + registered_expansion_count        |
|  - Target DB  (1 JSON query): source columns, function safety, 6 dest tables      |
|    (existence, row_counts, triggers/rules/RLS), raw_id bounds & checkpoints       |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|          2. CHUNKED SINGLE-PASS PARALLEL STAGING (public.pgdm_stage_unified)      |
|  - Partition raw_id into chunks of 1,000,000 rows (Parallel Index Scan on >1M)    |
|  - 6 Parallel Workers scan public.raw_data ONCE via jsonb_to_record(raw->'values')|
|  - Single 15-tuple VALUES unpivot emits pos=0 (base row) + pos=1..14 (valid axes) |
|  - Computes max_pos via flat GREATEST(integer_column_2..15) and t4_id arithmetic  |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|        3. IN-MEMORY SURROGATE KEY MAPPING & SET-BASED RELATIONAL INSERTS          |
|  - table3 & table4: Populated from pgdm_stage_unified (pos = 0) with NOT EXISTS   |
|  - table1: INSERT ... RETURNING (raw_id, t1_id) -> pg_temp.pgdm_t1_map (16B/row)  |
|  - table5, table6, table2: Single-statement Hash Joins against pgdm_t1_map        |
|  - Sequence CACHE 100k/250k + setval() synchronization (0 sequence gaps)          |
|  - Atomic per-chunk checkpoint written to public.pgdm_expansion_checkpoints       |
+-----------------------------------------------------------------------------------+
```

### 3.1 Single-Pass Parallel Unpack (`public.pgdm_stage_unified`)
Instead of scanning `public.raw_data` 19 times and calling `jsonb_extract_path_text` 730M times, we execute a single parallel `CREATE UNLOGGED TABLE public.pgdm_stage_unified AS SELECT ...` (`max_parallel_workers_per_gather = 6`):
1. **Single C-Level JSONB Parse:** `CROSS JOIN LATERAL jsonb_to_record(s.raw -> 'values') AS r(...)` parses the 64-key JSONB object in a single C-level pass per row.
2. **Unified 15-Way Unpivot (`pos = 0..14`):** A single `CROSS JOIN LATERAL (VALUES (0, ...), (1, ...), ..., (14, ...))` emits:
   - `pos = 0`: The base entity attributes for `raw_hash_manifest`, `table3`, `table4`, `table1`, `table5`, and `table6`.
   - `pos = 1..14`: Only valid child axis rows matching `decl_txt <> '' AND decl_txt::smallint = pos AND w_txt::numeric >= 0 AND UPPER(BTRIM(t_txt)) IN ('X', 'Y', 'Z')` for `table2`.
3. **Branch-Free `GREATEST(...)` for `max_pos`:** Replaces the correlated 14-row `LATERAL` subquery with `GREATEST(NULLIF(BTRIM(r.integer_column_2), '')::smallint, ..., NULLIF(BTRIM(r.integer_column_15), '')::smallint)`.
4. **Why `UNLOGGED` Instead of `TEMP`:** In PostgreSQL, `CREATE TEMP TABLE ... AS SELECT` cannot use parallel workers because temporary buffers are backend-private. Using an `UNLOGGED` table enables **6 parallel workers** during the JSONB unpack while avoiding WAL write overhead, and the table is dropped before `COMMIT`.

### 3.2 Direct Surrogate-Key Capture (`pg_temp.pgdm_t1_map`)
In the baseline pipeline, `table5`, `table6`, and `table2` all joined back against `public.table1` (`ON p.id_column_3 = s.raw_id`), and `table6` also joined against `public.table4` (`ON c.text_column_2 = ...`).
- We eliminate both joins by:
  1. Precomputing `t4_id = ((ASCII(LEFT(tc3, 1)) - 65) * 26 + (ASCII(RIGHT(tc3, 1)) - 65) + 1)::smallint` directly in `pgdm_stage_unified`.
  2. Using `WITH ins AS (INSERT INTO public.table1 ... RETURNING id_column_3 AS raw_id, id_column_1 AS t1_id) INSERT INTO pg_temp.pgdm_t1_map SELECT raw_id, t1_id FROM ins`.
- `pg_temp.pgdm_t1_map` is only `16 bytes/row` (`~16 MB` per 1M rows), fitting completely inside `temp_buffers = '256MB'` and `work_mem = '256MB'`.
- Combined with `ANALYZE pg_temp.pgdm_t1_map`, `SET LOCAL enable_mergejoin = off`, and `SET LOCAL enable_memoize = off`, `table5`, `table6`, and `table2` execute pure in-memory `Hash Join` plans with **zero sort/hash temp disk spill**.

### 3.3 Sequence Caching & `session_replication_role = 'replica'`
- Inside the protected expansion transaction, foreign key integrity is structurally guaranteed:
  - `table1.id_column_3` comes directly from `raw_data.raw_id`
  - `table1.id_column_4` comes from `JOIN public.table3`
  - `table5.id_column_2`, `table6.id_column_2`, and `table2.id_column_2` come from `JOIN pg_temp.pgdm_t1_map` (`table1.id_column_1`)
  - `table6.id_column_3` comes from `t4_id` (inserted into `public.table4` in the preceding step)
- Setting `SET LOCAL session_replication_role = 'replica'` inside the transaction skips 6.32M redundant per-row `RI_FKey_check_ins` trigger queries per 1M rows while keeping all `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, and `CHECK` constraints active.
- Raising sequence caches (`CACHE 100000` for `table1/5/6`, `CACHE 250000` for `table2`) during the chunk and resetting `CACHE 1` + `setval(...)` to `MAX(id_column_1)` at chunk boundaries eliminates sequence contention while guaranteeing **zero sequence gaps (`1..N`)**.

---

## 4. Failure-Recovery, Chunking & Checkpoint Strategy

Real-world data pipelines must survive connection drops, client crashes, and mid-flight failures without leaving partial duplicates or requiring manual cleanup. We designed a **3-Scenario Self-Healing Checkpoint Architecture** backed by `public.pgdm_expansion_checkpoints`:

```sql
CREATE TABLE IF NOT EXISTS public.pgdm_expansion_checkpoints (
    expansion_signature text NOT NULL,
    chunk_index integer NOT NULL,
    total_chunks integer NOT NULL,
    raw_id_min bigint,
    raw_id_max bigint,
    manifest_json jsonb NOT NULL,
    timings_json jsonb NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (expansion_signature, chunk_index)
);
```

### How `expansion_signature` Prevents Stale Replay
- During preflight, the service computes a deterministic SHA-256 `expansion_signature` over:
  1. `database_name`, `source_schema_name`, `source_table_name`
  2. Canonical `raw_schemas` and `source_versions`
  3. Destination table names and normalized SQL recipes
  4. `registered_expansion_count` (the current count of `cross_table_expansion` versions in `CONTROL_DB`)
- Whenever a run completes and registers its versions in `CONTROL_DB`, `registered_expansion_count` increments, automatically invalidating old checkpoints for any future deliberate re-execution.

### Recovery Across the 3 Failure Scenarios

| Failure Scenario | What Happens During Crash | Automatic Recovery Behavior on Retry |
| :--- | :--- | :--- |
| **Scenario A: Single-Chunk Mid-Flight Crash (`\le 1\text{M}` rows)** | PostgreSQL automatically rolls back the uncommitted transaction; `pgdm_stage_unified` and temp tables vanish. Even if sequences were advanced before rollback, `setval(...)` resynchronizes sequences at the start of the retry. | Cleanly re-runs from `raw_id = 1` with **0 duplicate rows and 0 sequence gaps**. |
| **Scenario B: Multi-Chunk Mid-Expansion Crash (`> 1\text{M}` rows, e.g., crash at Chunk $k$ of $N$)** | Chunks $1 \dots k-1$ are committed atomically alongside their rows in `public.pgdm_expansion_checkpoints`. Chunk $k$ rolls back cleanly. | Preflight reads `public.pgdm_expansion_checkpoints`, loads the saved manifests/timings for Chunks $1 \dots k-1$, resets sequences to `MAX(id_column_1)` of committed rows, and **resumes execution at Chunk $k$**. Dimension tables (`table3`, `table4`) use `WHERE NOT EXISTS` so existing dimension keys are reused seamlessly. |
| **Scenario C: Post-Data-Commit / Pre-Versioning Crash** | All chunks $1 \dots N$ committed in `kaggle_challenge`, but the client or network crashed before `register_cross_table_expansion_versions` committed in `CONTROL_DB`. | On retry, `registered_expansion_count` is unchanged, so `expansion_signature` matches all $N$ committed checkpoints in `public.pgdm_expansion_checkpoints`. `_execute_fast_challenge_expansion` **skips all data SQL (`0` rows re-inserted in `<120 ms`)**, reconstructs the full manifest from the checkpoints, and completes `CONTROL_DB` version registration. |

---

## 5. Empirical Benchmark Results (`100k`, `1M`, `10M` Rows)

###Test Environment
- **Hardware:** Apple Mac Mini M4 (10-core CPU, 24 GB Unified Memory)
- **Database Engine:** PostgreSQL 16 (`shared_buffers = 2GB`, `max_worker_processes = 12`, `max_parallel_workers = 10`, `max_parallel_workers_per_gather = 6`, `max_wal_size = 8GB`)
- **Dataset Generator:** `generate_database.py` (`FIXED_SEED = 4918304907611952177`, `raw_schema = 'group001'`)

### 5.1 Stage-by-Stage Comparison (`100,000` and `1,000,000` Rows)

| Pipeline Stage / Metric | `100k` Baseline | `100k` Optimized | `100k` Speedup | `1M` Baseline | `1M` Optimized | `1M` Speedup |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Source Rows (`public.raw_data`)** | `100,000` | `100,000` | — | `1,000,000` | `1,000,000` | — |
| **Total Inserted Rows (`table1..table6`)** | `532,416` | `532,416` | — | `5,321,553` | `5,321,553` | — |
| **1. Preparation Subtotal** | `53.300 ms` | **`32.350 ms`** | `1.65x` | `44.536 ms` | **`38.948 ms`** | `1.14x` |
| **2. Preflight Validation Subtotal** | `1,147.000 ms` | **`58.297 ms`** | **`19.68x`** | `523.770 ms` | **`73.163 ms`** | **`7.16x`** |
| **3. PostgreSQL Expansion Subtotal** | `8,290.000 ms` | **`2,522.000 ms`** | **`3.29x`** | `174,316.000 ms` | **`28,744.000 ms`** | **`6.06x`** |
| — *Scan source & build manifest / stage* | `348.000 ms` | `614.670 ms` *(incl. full unpack)* | — | `4,255.000 ms` | `5,677.000 ms` *(incl. full unpack)* | — |
| — *Insert `public.table3` (`4` rows)* | `224.000 ms` | **`55.786 ms`** | **`4.02x`** | `2,806.000 ms` | **`614.440 ms`** | **`4.57x`** |
| — *Insert `public.table4` (`26–27` rows)* | `89.000 ms` | **`25.756 ms`** | **`3.46x`** | `1,062.000 ms` | **`311.489 ms`** | **`3.41x`** |
| — *Insert `public.table1` (`100k / 1M` rows)* | `4,120.000 ms` | **`385.608 ms`** | **`10.68x`** | `98,975.000 ms` | **`4,972.000 ms`** | **`19.91x`** |
| — *Insert `public.table5` (`100k / 1M` rows)* | `680.000 ms` | **`318.448 ms`** | **`2.14x`** | `14,371.000 ms` | **`3,610.000 ms`** | **`3.98x`** |
| — *Insert `public.table6` (`100k / 1M` rows)* | `740.000 ms` | **`348.594 ms`** | **`2.12x`** | `15,455.000 ms` | **`4,640.000 ms`** | **`3.33x`** |
| — *Insert `public.table2` (`232k / 2.32M` rows)* | `2,085.000 ms` *(14 stmts)* | **`770.000 ms`** *(1 stmt)* | **`2.71x`** | `37,383.000 ms` *(14 stmts)* | **`8,917.000 ms`** *(1 stmt)* | **`4.19x`** |
| **4. Version Registration Subtotal** | `80.100 ms` | **`18.082 ms`** | **`4.43x`** | `22.151 ms` | **`17.366 ms`** | **`1.28x`** |
| **5. Interface Refresh Subtotal** | `84.600 ms` | **`26.139 ms`** | **`3.24x`** | `30.231 ms` | **`29.085 ms`** | **`1.04x`** |
| **MEASURED PROCESSING TOTAL** | **`9.655 s`** | **`2.657 s`** | **`3.63x`** | **`174.937 s`** | **`28.903 s`** | **`6.05x`** |
| **PostgreSQL Temp Disk Spill** | `284.00 MB` | **`0.06 MB`** | **`4,544x` less** | `2,739.71 MB` | **`0.06 MB`** | **`41,804x` less** |

### 5.2 Large-Scale `10,000,000`-Row (`10M` $\rightarrow$ `53,225,999` Inserted Rows) Benchmark

On `10,000,000` source rows (`19.67 GiB` `public.raw_data` table), our pipeline automatically partitions the workload into **10 checkpointed chunks of `1,000,000` rows**:

| Metric / Stage | `10,000,000`-Row Optimized Result |
| :--- | :--- |
| **Source Rows (`public.raw_data`)** | `10,000,000` (`20,137.88 MB`) |
| **Total Inserted Destination Rows** | **`53,225,999`** (`table3: 4`, `table4: 27`, `table1: 10,000,000`, `table5: 10,000,000`, `table6: 10,000,000`, `table2: 23,225,968`) |
| **1. Preparation Subtotal (10 Chunks)** | **`63.074 ms`** |
| **2. Preflight Validation Subtotal** | **`83.962 ms`** |
| **3. PostgreSQL Expansion Subtotal (10 Chunks)** | **`444.818 s (7m 24.818s)`** |
| — *Scan source & build unified stage + manifest* | `239.874 s (3m 59.874s)` |
| — *Insert `public.table3` (`4` rows)* | `5.506 s` |
| — *Insert `public.table4` (`27` rows)* | `2.365 s` |
| — *Insert `public.table1` (`10,000,000` rows)* | **`40.718 s` (`245,592 rows/s`)** |
| — *Insert `public.table5` (`10,000,000` rows)* | **`33.167 s` (`301,504 rows/s`)** |
| — *Insert `public.table6` (`10,000,000` rows)* | **`33.933 s` (`294,698 rows/s`)** |
| — *Insert `public.table2` (`23,225,968` rows)* | **`88.332 s` (`262,939 rows/s`)** |
| — *Commit expansion transactions (10 Chunks)* | `816.280 ms` |
| **4. Version Registration Subtotal** | **`40.340 ms`** |
| **5. Interface Refresh Subtotal** | **`26.995 ms`** |
| **MEASURED PROCESSING TOTAL** | **`445.033 s (7m 25.033s)`** (`~119,600 inserted rows/sec` sustained) |
| **Total Temp Disk Spill Across 10 Chunks** | **`0.63 MB` (`655,360 bytes` total — `64 KiB` per chunk!)** |
| **Final Database Size (`kaggle_challenge`)** | `26,164.41 MB` (`25.55 GiB`) |

---

## 6. Verification of Data Integrity & Trade-Offs

### Bit-for-Bit Checksum Equivalence
On both `100,000` rows (`532,416` destination rows) and `1,000,000` rows (`5,321,553` destination rows), we computed full-table 128-bit MD5 XOR fingerprints across every column of `table3`, `table4`, `table1`, `table5`, `table6`, and `table2` (joining surrogate foreign keys back to natural keys `raw_id` and `table3` natural attributes) as well as `public.pgdm_table_row_counts`:

| Table | `1,000,000`-Row Inserted Count | Baseline 128-bit Checksum (`h1:h2`) | Optimized 128-bit Checksum (`h1:h2`) | Match |
| :--- | :--- | :--- | :--- | :--- |
| `public.table3` | `4` | `5910328308618162165:7916243977308018310` | `5910328308618162165:7916243977308018310` | **EXACT** |
| `public.table4` | `27` | `-2419822977198407780:-2818916562354032807` | `-2419822977198407780:-2818916562354032807` | **EXACT** |
| `public.table1` | `1,000,000` | `-1641517583254132145:-2282384621122922416` | `-1641517583254132145:-2282384621122922416` | **EXACT** |
| `public.table5` | `1,000,000` | `-6159195294265460509:305788924127984862` | `-6159195294265460509:305788924127984862` | **EXACT** |
| `public.table6` | `1,000,000` | `677255175827664021:-2350261280673957099` | `677255175827664021:-2350261280673957099` | **EXACT** |
| `public.table2` | `2,321,522` | `-1292117238050606431:7421737121719507036` | `-1292117238050606431:7421737121719507036` | **EXACT** |

### Engineering Trade-Offs
1. **Unlogged Staging Table vs. Temp Table:**
   - *Trade-off:* PostgreSQL `TEMP` tables cannot be written or scanned by parallel workers (`max_parallel_workers_per_gather`), whereas `UNLOGGED` tables support full parallel `CTAS` and parallel `Seq Scan` while still bypassing WAL logging. We drop `public.pgdm_stage_unified` inside the same transaction before `COMMIT`, leaving zero residual storage footprint.
2. **Chunk Size (`1,000,000` rows per chunk):**
   - *Trade-off:* A `1,000,000`-row chunk size ensures `100k` and `1M` datasets execute in a single atomic transaction with zero multi-chunk overhead, while bounding `pgdm_stage_unified` (`~320 MB`) and `pg_temp.pgdm_t1_map` (`~16 MB`) on `10M+` datasets so every Hash Join fits 100% in memory (`work_mem = 256MB`) with `<64 KiB` temp spill per chunk and fine-grained `~44s` crash-recovery checkpoints.
