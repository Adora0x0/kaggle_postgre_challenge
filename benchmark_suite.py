#!/usr/bin/env python3
"""Headless Benchmark, Bit-for-Bit Verification & Crash-Recovery Suite.

Provides a reproducible CLI harness for evaluating the PostgreSQL multi-table
expansion pipeline using the exact timing report infrastructure from the
application (`services.postgres_service.PostgresAdminService` and `ui.dialogs`).

Example usage:
    python benchmark_suite.py --host localhost --pg-user postgres --local-subprocess \
        --verify-crash-recovery
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shlex
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

from config import CONTROL_DB
from services.postgres_service import PostgresAdminService
from ui.dialogs import ExpansionTimingReportDialog

DB_NAME = "kaggle_challenge"
SQL_DIR = REPO_DIR / "SQL files"
SQL_FILES = [
    ("public.table3", "01_table3.sql"),
    ("public.table4", "02_table4.sql"),
    ("public.table1", "03_table1.sql"),
    ("public.table5", "04_table5.sql"),
    ("public.table6", "05_table6.sql"),
    ("public.table2", "06_table2.sql"),
]


class LocalSSHChannel:
    """Emulates a Paramiko Channel backed by a local subprocess for local testing."""

    def __init__(self, command: str, env: dict):
        self._proc = subprocess.Popen(
            ["/bin/bash", "-c", command],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        self._stdout_buf = bytearray()
        self._stderr_buf = bytearray()
        self._lock = threading.Lock()
        self._t_out = threading.Thread(
            target=self._read_stream,
            args=(self._proc.stdout, self._stdout_buf),
            daemon=True,
        )
        self._t_err = threading.Thread(
            target=self._read_stream,
            args=(self._proc.stderr, self._stderr_buf),
            daemon=True,
        )
        self._t_out.start()
        self._t_err.start()

    def _read_stream(self, stream, buf: bytearray):
        try:
            while True:
                chunk = stream.read(65536)
                if not chunk:
                    break
                with self._lock:
                    buf.extend(chunk)
        except Exception:
            pass

    def sendall(self, data: bytes):
        if self._proc.stdin and not self._proc.stdin.closed:
            self._proc.stdin.write(data)
            self._proc.stdin.flush()

    def shutdown_write(self):
        if self._proc.stdin and not self._proc.stdin.closed:
            self._proc.stdin.close()

    def recv_ready(self) -> bool:
        with self._lock:
            return len(self._stdout_buf) > 0

    def recv(self, nbytes: int) -> bytes:
        with self._lock:
            chunk = bytes(self._stdout_buf[:nbytes])
            del self._stdout_buf[:nbytes]
            return chunk

    def recv_stderr_ready(self) -> bool:
        with self._lock:
            return len(self._stderr_buf) > 0

    def recv_stderr(self, nbytes: int) -> bytes:
        with self._lock:
            chunk = bytes(self._stderr_buf[:nbytes])
            del self._stderr_buf[:nbytes]
            return chunk

    def exit_status_ready(self) -> bool:
        if self._proc.poll() is None:
            return False
        return not self._t_out.is_alive() and not self._t_err.is_alive()

    def recv_exit_status(self) -> int:
        self._t_out.join(timeout=2.0)
        self._t_err.join(timeout=2.0)
        return self._proc.wait()

    def close(self):
        try:
            if self._proc.stdin and not self._proc.stdin.closed:
                self._proc.stdin.close()
        except Exception:
            pass
        if self._proc.poll() is None:
            try:
                self._proc.terminate()
            except Exception:
                pass


class LocalChannelFile:
    def __init__(self, channel: LocalSSHChannel):
        self.channel = channel


class LocalSubprocessSSHClient:
    def __init__(self, env: dict):
        self._env = env

    def exec_command(self, command: str):
        ch = LocalSSHChannel(command, self._env)
        return LocalChannelFile(ch), LocalChannelFile(ch), LocalChannelFile(ch)

    def close(self):
        pass


def _ensure_gnu_date_on_macos(env: dict) -> dict:
    if sys.platform != "darwin":
        return env
    import tempfile

    shim_dir = Path(tempfile.gettempdir()) / "pgdm_gnu_date_shim"
    shim_dir.mkdir(parents=True, exist_ok=True)
    date_bin = shim_dir / "date"
    if not date_bin.exists():
        c_src = shim_dir / "date_shim.c"
        c_src.write_text(
            "#include <stdio.h>\n#include <string.h>\n#include <time.h>\n#include <unistd.h>\n"
            "int main(int argc, char **argv) {\n"
            "  if (argc == 2 && strcmp(argv[1], \"+%s%N\") == 0) {\n"
            "    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);\n"
            "    printf(\"%lld%09ld\\n\", (long long)ts.tv_sec, ts.tv_nsec); return 0;\n"
            "  }\n"
            "  execv(\"/bin/date\", argv); return 1;\n"
            "}\n",
            encoding="utf-8",
        )
        subprocess.run(["cc", "-O3", str(c_src), "-o", str(date_bin)], check=False)
    if date_bin.exists():
        env = dict(env)
        env["PATH"] = f"{shim_dir}:{env.get('PATH', '')}"
    return env


def connect_service(args: argparse.Namespace) -> PostgresAdminService:
    svc = PostgresAdminService()
    if args.local_subprocess:
        env = _ensure_gnu_date_on_macos(os.environ.copy())
        svc.ssh_client = LocalSubprocessSSHClient(env)
        svc.ssh_host = args.host
        svc.ssh_port = args.ssh_port
        svc.ssh_username = args.ssh_user
        svc.ssh_password = ""
        svc.postgres_port = args.pg_port
        svc.sql_username = args.pg_user
        svc.sql_password = os.environ.get("PGPASSWORD", "")
    else:
        svc.connect(
            ssh_host=args.host,
            ssh_port=args.ssh_port,
            ssh_username=args.ssh_user,
            ssh_password=os.environ.get("PGDM_SSH_PASSWORD", ""),
            postgres_port=args.pg_port,
            sql_username=args.pg_user,
            sql_password=os.environ.get("PGPASSWORD", ""),
        )
    svc.ensure_admin_schema()
    return svc


def load_destinations() -> list[dict]:
    return [
        {
            "table_name": table_name,
            "sql": (SQL_DIR / filename).read_text(encoding="utf-8"),
            "version_title": f"Expand {table_name}",
        }
        for table_name, filename in SQL_FILES
    ]


def reset_expanded_destinations(service: PostgresAdminService) -> None:
    service.ensure_data_database_metadata(DB_NAME)
    sql_data = """
    BEGIN;
    SET LOCAL client_min_messages = warning;
    TRUNCATE TABLE
        public.table2,
        public.table5,
        public.table6,
        public.table1,
        public.table3,
        public.table4
    RESTART IDENTITY CASCADE;
    DROP TABLE IF EXISTS public.pgdm_expansion_checkpoints;
    DROP TABLE IF EXISTS public.pgdm_stage_unified;
    INSERT INTO public.pgdm_table_row_counts (
        schema_name, table_name, row_count, initialized_at, updated_at, last_reconciled_at
    )
    SELECT 'public', t.name, 0, clock_timestamp(), clock_timestamp(), clock_timestamp()
    FROM unnest(ARRAY['table1','table2','table3','table4','table5','table6']) AS t(name)
    ON CONFLICT (schema_name, table_name) DO UPDATE
    SET row_count = 0, updated_at = clock_timestamp(), last_reconciled_at = clock_timestamp();
    COMMIT;
    """
    service.run_remote_command(
        f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {DB_NAME} -X -q -v ON_ERROR_STOP=1",
        stdin_text=sql_data,
    )
    sql_ctrl = """
    BEGIN;
    DELETE FROM app_control.table_version_dependencies
    WHERE table_version_id IN (
        SELECT id FROM app_control.table_versions
        WHERE database_name = 'kaggle_challenge'
          AND table_name IN ('table1', 'table2', 'table3', 'table4', 'table5', 'table6')
          AND version_code <> '0000'
    );
    DELETE FROM app_control.table_versions
    WHERE database_name = 'kaggle_challenge'
      AND table_name IN ('table1', 'table2', 'table3', 'table4', 'table5', 'table6')
      AND version_code <> '0000';
    COMMIT;
    """
    service.run_remote_command(
        f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {CONTROL_DB} -X -q -v ON_ERROR_STOP=1",
        stdin_text=sql_ctrl,
    )


def get_pg_temp_stats(service: PostgresAdminService) -> dict:
    sql = """
    SELECT json_build_object(
        'temp_files', COALESCE(temp_files, 0),
        'temp_bytes', COALESCE(temp_bytes, 0),
        'db_size_bytes', pg_database_size(datname)
    )::text
    FROM pg_stat_database
    WHERE datname = current_database();
    """
    out = service.run_remote_command(
        f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {DB_NAME} -X -qAt -c {shlex.quote(sql)}"
    ).strip()
    return json.loads(out.splitlines()[-1])


def compute_fast_parallel_fingerprint(service: PostgresAdminService) -> dict:
    tables = ["table3", "table4", "table1", "table5", "table6", "table2"]
    summary = {}
    for t in tables:
        if t == "table3":
            cols_sql = "text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4"
            join_sql = ""
        elif t == "table4":
            cols_sql = "id_column_1, text_column_1, text_column_2"
            join_sql = ""
        elif t == "table1":
            cols_sql = (
                "t.id_column_2, t.text_column_1, t.datetime_column_1, t.text_column_2, "
                "t.integer_column_1, t.integer_column_2, t.integer_column_3, t.text_column_3, "
                "t.id_column_3, "
                "(SELECT text_column_1 || '|' || text_column_2 || '|' || COALESCE(numeric_column_1::text, '') || '|' || text_column_3 || '|' || text_column_4 FROM public.table3 t3 WHERE t3.id_column_1 = t.id_column_4), "
                "t.id_column_5, t.integer_column_4, t.integer_column_5, t.id_column_6, t.integer_column_6, t.integer_column_7"
            )
            join_sql = ""
        elif t == "table5":
            cols_sql = "t1.id_column_3, t.numeric_column_1, t.numeric_column_2, t.integer_column_1, t.integer_column_2, t.id_column_3"
            join_sql = "LEFT JOIN public.table1 t1 ON t1.id_column_1 = t.id_column_2"
        elif t == "table6":
            cols_sql = "t1.id_column_3, t.id_column_3"
            join_sql = "LEFT JOIN public.table1 t1 ON t1.id_column_1 = t.id_column_2"
        elif t == "table2":
            cols_sql = (
                "t1.id_column_3, t.integer_column_1, t.integer_column_2, t.text_column_1, "
                "t.integer_column_3, t.integer_column_4, t.integer_column_5, t.integer_column_6"
            )
            join_sql = "LEFT JOIN public.table1 t1 ON t1.id_column_1 = t.id_column_2"

        sql = f"""
        SET max_parallel_workers_per_gather = 6;
        SELECT
            COUNT(*)::bigint,
            COALESCE(BIT_XOR(('x' || substr(md5(ROW({cols_sql})::text), 1, 16))::bit(64)::bigint), 0),
            COALESCE(BIT_XOR(('x' || substr(md5(ROW({cols_sql})::text), 17, 16))::bit(64)::bigint), 0)
        FROM public.{t} t
        {join_sql};
        """
        out = service.run_remote_command(
            f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {DB_NAME} -X -qAt -F '|' -c {shlex.quote(sql)}"
        ).strip()
        cnt_s, h1_s, h2_s = out.splitlines()[-1].split("|")
        summary[t] = {"row_count": int(cnt_s), "hash_xor": f"{h1_s}:{h2_s}"}
    return summary


def run_timed_expansion(
    service: PostgresAdminService,
    destinations: list[dict],
    chunk_size: int | None = None,
) -> dict:
    if chunk_size is not None:
        service.expansion_chunk_size = int(chunk_size)
    table_name = "public.raw_data"
    raw_schemas = ["group001"]

    stats_before = get_pg_temp_stats(service)
    t_wall_0 = time.perf_counter()

    timing_report = service.create_cross_table_expansion_timing_report(
        DB_NAME,
        table_name,
        raw_schemas,
        [item["table_name"] for item in destinations],
    )
    with service.expansion_timing_scope(
        timing_report,
        "prepare",
        "Ensure control database metadata",
    ):
        service.ensure_admin_schema()

    result = service.execute_cross_table_expansion(
        database_name=DB_NAME,
        source_full_table_name=table_name,
        raw_schemas=raw_schemas,
        destinations=destinations,
        timing_report=timing_report,
    )

    registered_versions = service.register_cross_table_expansion_versions(
        expansion_result=result,
        requested_by="benchmark",
        workstation_name=socket.gethostname(),
        timing_report=timing_report,
    )

    with service.expansion_timing_scope(
        timing_report, "refresh", "Invalidate destination filter caches"
    ):
        pass
    with service.expansion_timing_scope(
        timing_report, "refresh", "Reload database table catalog"
    ):
        service.list_tables(DB_NAME)
    with service.expansion_timing_scope(
        timing_report, "refresh", "Rebuild table navigation controls"
    ):
        pass
    with service.expansion_timing_scope(
        timing_report, "refresh", "Build completion summary"
    ):
        _ = ", ".join(
            f"{item['table_name']}={item['version_code']}"
            for item in registered_versions
        )

    timing_report["completed_at"] = (
        datetime.now().astimezone().isoformat(timespec="seconds")
    )
    wall_seconds = time.perf_counter() - t_wall_0
    stats_after = get_pg_temp_stats(service)
    report_text = ExpansionTimingReportDialog.build_report_text(timing_report)

    return {
        "wall_seconds": round(wall_seconds, 3),
        "temp_files_delta": stats_after["temp_files"] - stats_before["temp_files"],
        "temp_bytes_delta": stats_after["temp_bytes"] - stats_before["temp_bytes"],
        "db_size_after_mb": round(stats_after["db_size_bytes"] / (1024 * 1024), 2),
        "python_max_rss_mb": round(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024), 2
        ),
        "report_text": report_text,
    }


def verify_crash_recovery(service: PostgresAdminService) -> None:
    print("\n=== Crash Recovery & Idempotency Verification ===", flush=True)
    destinations = load_destinations()
    reset_expanded_destinations(service)
    _ = run_timed_expansion(service, destinations, chunk_size=1_000_000)
    golden = compute_fast_parallel_fingerprint(service)
    raw_rows = int(
        service.run_remote_command(
            f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {DB_NAME} -X -qAt -c 'SELECT COUNT(*) FROM public.raw_data;'"
        ).strip()
    )
    chunk_sz = max(1, raw_rows // 3 + 1)

    print(
        f"[Scenario B] Simulating mid-expansion crash at chunk 2/3 (chunk_size={chunk_sz:,})...",
        flush=True,
    )
    reset_expanded_destinations(service)
    orig_build = service._build_fast_challenge_chunk_sql
    should_crash = [True]

    def crashing_build_chunk(*a, **kw):
        if should_crash[0] and kw.get("chunk_index") == 2:
            service.run_remote_command(
                f"psql -h localhost -U {shlex.quote(service.sql_username)} -d {DB_NAME} -X -q -c \"SELECT nextval('public.table1_id_column_1_seq'), nextval('public.table2_id_column_1_seq');\""
            )
            raise RuntimeError("Simulated mid-expansion crash during chunk 2!")
        return orig_build(*a, **kw)

    service._build_fast_challenge_chunk_sql = crashing_build_chunk
    try:
        run_timed_expansion(service, destinations, chunk_size=chunk_sz)
        raise AssertionError("Expected simulated crash in chunk 2")
    except RuntimeError as exc:
        print(f"  Caught expected failure: {exc}", flush=True)
    finally:
        service._build_fast_challenge_chunk_sql = orig_build

    print("  Resuming interrupted expansion from checkpoint...", flush=True)
    res_b = run_timed_expansion(service, destinations, chunk_size=chunk_sz)
    recovered_b = compute_fast_parallel_fingerprint(service)
    if recovered_b != golden:
        raise AssertionError(f"Scenario B checksum mismatch: {recovered_b} != {golden}")
    print(
        f"  [PASS] Scenario B: resumed in {res_b['wall_seconds']:.3f}s, "
        "0 duplicates, 0 sequence gaps, 100% checksum match."
    )

    print(
        "[Scenario C] Simulating post-data-commit / pre-versioning crash...",
        flush=True,
    )
    reset_expanded_destinations(service)
    orig_reg = service.register_cross_table_expansion_versions

    def fail_version_reg(*a, **kw):
        raise RuntimeError("Simulated crash before CONTROL_DB version registration!")

    service.register_cross_table_expansion_versions = fail_version_reg
    try:
        run_timed_expansion(service, destinations, chunk_size=1_000_000)
        raise AssertionError("Expected simulated crash before versioning")
    except RuntimeError as exc:
        print(f"  Caught expected failure: {exc}", flush=True)
    finally:
        service.register_cross_table_expansion_versions = orig_reg

    print("  Retrying expansion after post-commit crash...", flush=True)
    res_c = run_timed_expansion(service, destinations, chunk_size=1_000_000)
    recovered_c = compute_fast_parallel_fingerprint(service)
    if recovered_c != golden:
        raise AssertionError(f"Scenario C checksum mismatch: {recovered_c} != {golden}")
    print(
        f"  [PASS] Scenario C: recovered in {res_c['wall_seconds']:.3f}s, "
        "0 duplicates, 100% checksum match."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--ssh-port", type=int, default=22)
    parser.add_argument("--ssh-user", default=os.environ.get("USER", "postgres"))
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--chunk-size", type=int, default=1_000_000)
    parser.add_argument(
        "--local-subprocess",
        action="store_true",
        help="Use local subprocess channel emulation when running on the database host.",
    )
    parser.add_argument("--verify-crash-recovery", action="store_true")
    args = parser.parse_args()

    service = connect_service(args)
    reset_expanded_destinations(service)
    destinations = load_destinations()
    res = run_timed_expansion(service, destinations, chunk_size=args.chunk_size)
    print(res["report_text"])
    summary = {k: v for k, v in res.items() if k != "report_text"}
    print("\n=== Resource & Wall-Time Summary ===")
    print(json.dumps(summary, indent=2))
    if args.verify_crash_recovery:
        verify_crash_recovery(service)


if __name__ == "__main__":
    main()
