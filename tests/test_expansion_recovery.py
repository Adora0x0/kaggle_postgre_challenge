from pathlib import Path
import unittest

from services.postgres_service import PostgresAdminService


SQL_DIR = Path(__file__).resolve().parents[1] / "SQL files"


def _load_challenge_destinations():
    mapping = [
        ("public.table3", "01_table3.sql"),
        ("public.table4", "02_table4.sql"),
        ("public.table1", "03_table1.sql"),
        ("public.table5", "04_table5.sql"),
        ("public.table6", "05_table6.sql"),
        ("public.table2", "06_table2.sql"),
    ]
    return [
        {
            "table_name": table_name,
            "version_title": f"Expand {table_name}",
            "sql": (SQL_DIR / filename).read_text(encoding="utf-8"),
        }
        for table_name, filename in mapping
    ]


class ExpansionRecoveryAndChunkingTests(unittest.TestCase):
    def test_standard_challenge_expansion_is_recognized(self):
        prepared = PostgresAdminService._prepare_cross_table_expansion(
            "public.raw_data",
            ["group001"],
            _load_challenge_destinations(),
        )
        self.assertTrue(
            PostgresAdminService._is_standard_challenge_expansion(prepared)
        )

    def test_merge_chunk_manifests_sums_row_counts_deterministically(self):
        m1 = {
            "raw_schemas": ["group001"],
            "expected_row_count": 40000,
            "expected_distinct_raw_hash_count": 1,
            "raw_hash_manifest": [
                {"raw_schema": "group001", "raw_hash": "abc123", "row_count": 40000}
            ],
            "source_columns": [{"name": "raw_id", "type": "bigint"}],
        }
        m2 = {
            "raw_schemas": ["group001"],
            "expected_row_count": 60000,
            "expected_distinct_raw_hash_count": 1,
            "raw_hash_manifest": [
                {"raw_schema": "group001", "raw_hash": "abc123", "row_count": 60000}
            ],
            "source_columns": [{"name": "raw_id", "type": "bigint"}],
        }
        merged = PostgresAdminService._merge_chunk_manifests([m1, m2])
        self.assertEqual(merged["expected_row_count"], 100000)
        self.assertEqual(merged["expected_distinct_raw_hash_count"], 1)
        self.assertEqual(
            merged["raw_hash_manifest"],
            [{"raw_schema": "group001", "raw_hash": "abc123", "row_count": 100000}],
        )

    def test_expansion_signature_changes_when_version_is_registered(self):
        service = PostgresAdminService()
        prepared = service._prepare_cross_table_expansion(
            "public.raw_data",
            ["group001"],
            _load_challenge_destinations(),
        )
        source_versions = [{"version_code": "0000", "raw_schema": "group001"}]
        sig_before = service._compute_expansion_signature(
            "kaggle_challenge",
            prepared,
            source_versions,
            registered_expansion_count=0,
        )
        sig_after = service._compute_expansion_signature(
            "kaggle_challenge",
            prepared,
            source_versions,
            registered_expansion_count=6,
        )
        self.assertNotEqual(sig_before, sig_after)

    def test_fast_challenge_chunk_sql_includes_range_bounds_and_checkpoint(self):
        service = PostgresAdminService()
        prepared = service._prepare_cross_table_expansion(
            "public.raw_data",
            ["group001"],
            _load_challenge_destinations(),
        )
        chunk_sql = service._build_fast_challenge_chunk_sql(
            source_schema_name="public",
            source_table_name="raw_data",
            raw_schemas=["group001"],
            destinations=prepared["destinations"],
            expansion_signature="sig_test_123",
            chunk_index=2,
            total_chunks=3,
            chunk_min_raw_id=40001,
            chunk_max_raw_id=80000,
        )
        self.assertIn("s.raw_id BETWEEN 40001 AND 80000", chunk_sql)
        self.assertIn("SET LOCAL enable_seqscan = off;", chunk_sql)
        self.assertIn("INSERT INTO public.pgdm_expansion_checkpoints", chunk_sql)
        self.assertIn("ALTER SEQUENCE public.table1_id_column_1_seq CACHE 100000;", chunk_sql)
        self.assertIn("$pgdm_fk_pre$", chunk_sql)
        self.assertIn("NOT VALID", chunk_sql)
        self.assertIn("DROP CONSTRAINT IF EXISTS table2_pkey;", chunk_sql)
        self.assertNotIn("ADD CONSTRAINT table2_pkey PRIMARY KEY", chunk_sql)

        final_chunk_sql = service._build_fast_challenge_chunk_sql(
            source_schema_name="public",
            source_table_name="raw_data",
            raw_schemas=["group001"],
            destinations=prepared["destinations"],
            expansion_signature="sig_test_123",
            chunk_index=3,
            total_chunks=3,
            chunk_min_raw_id=80001,
            chunk_max_raw_id=100000,
        )
        self.assertNotIn("NOT VALID", final_chunk_sql)
        self.assertIn("SET LOCAL max_parallel_maintenance_workers = 0;", final_chunk_sql)
        self.assertIn("SET LOCAL maintenance_work_mem = '512MB';", final_chunk_sql)
        self.assertIn("ADD CONSTRAINT table2_pkey PRIMARY KEY (id_column_1)", final_chunk_sql)

    def test_custom_modified_recipe_uses_general_path(self):
        dests = _load_challenge_destinations()
        dests[0]["sql"] = dests[0]["sql"].replace("SYNTHETIC/REFERENCE", "CUSTOM/OVERRIDE")
        prepared = PostgresAdminService._prepare_cross_table_expansion(
            "public.raw_data",
            ["group001"],
            dests,
        )
        self.assertFalse(
            PostgresAdminService._is_standard_challenge_expansion(prepared)
        )

    def test_list_tables_hides_internal_expansion_tables(self):
        service = PostgresAdminService()
        service.run_remote_command = lambda cmd, cancel_event=None: (
            "public.pgdm_expansion_checkpoints\n"
            "public.pgdm_stage_unified\n"
            "public.pgdm_table_row_counts\n"
            "public.raw_data\n"
            "public.table1\n"
        )
        self.assertEqual(
            service.list_tables("kaggle_challenge"),
            ["public.raw_data", "public.table1"],
        )


if __name__ == "__main__":
    unittest.main()
