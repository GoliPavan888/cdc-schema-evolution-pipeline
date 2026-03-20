"""Local unit tests for CDC processor components (no Docker required)."""

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Add processor src to path
sys.path.insert(0, str(Path(__file__).parent / "processor" / "src"))

from schema_store import SchemaStore, utc_now_iso


def test_schema_store_initialization():
    """Test schema store DB initialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/test.db"
        store = SchemaStore(db_path)
        
        # Verify tables exist
        tables = []
        conn = store._connect()
        try:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
        
        assert "table_schemas" in tables, "table_schemas missing"
        assert "schema_partitions" in tables, "schema_partitions missing"
        print("✓ Schema store initialization")


def test_schema_registration():
    """Test schema registration and versioning."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SchemaStore(f"{tmpdir}/test.db")
        now = utc_now_iso()
        
        # First schema for products
        schema_v1 = {"id": "int", "name": "string", "description": "string", "price": "double"}
        v1 = store.get_or_register_schema("products", schema_v1, now)
        assert v1 == 1, f"Expected version 1, got {v1}"
        
        # Same schema again should return same version
        v1_again = store.get_or_register_schema("products", schema_v1, now)
        assert v1_again == 1, f"Expected version 1 again, got {v1_again}"
        
        # Different schema (renamed column) should create v2
        schema_v2 = {"id": "int", "name": "string", "product_description": "string", "price": "double"}
        v2 = store.get_or_register_schema("products", schema_v2, now)
        assert v2 == 2, f"Expected version 2, got {v2}"
        
        # Same v2 schema again
        v2_again = store.get_or_register_schema("products", schema_v2, now)
        assert v2_again == 2, f"Expected version 2 again, got {v2_again}"
        
        print("✓ Schema registration and versioning")


def test_schema_per_table():
    """Test independent versioning per table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SchemaStore(f"{tmpdir}/test.db")
        now = utc_now_iso()
        
        products_schema = {"id": "int", "name": "string", "price": "double"}
        customers_schema = {"id": "int", "email": "string", "created_at": "string"}
        
        pv1 = store.get_or_register_schema("products", products_schema, now)
        cv1 = store.get_or_register_schema("customers", customers_schema, now)
        
        assert pv1 == 1, "Products should be version 1"
        assert cv1 == 1, "Customers should be version 1"
        
        # Both tables have their own version tracks
        products_schema_v2 = {"id": "int", "name": "string", "title": "string", "price": "double"}
        pv2 = store.get_or_register_schema("products", products_schema_v2, now)
        
        assert pv2 == 2, "Products v2 should be 2"
        cv1_check = store.get_or_register_schema("customers", customers_schema, now)
        assert cv1_check == 1, "Customers should still be 1"
        
        print("✓ Independent schema versioning per table")


def test_partition_tracking():
    """Test partition tracking for schema versions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SchemaStore(f"{tmpdir}/test.db")
        now = utc_now_iso()
        
        schema = {"id": "int", "name": "string"}
        v = store.get_or_register_schema("products", schema, now)
        
        # Track partitions
        store.track_partition("products", v, "/data_lake/products/2026-03-19/c/", now)
        store.track_partition("products", v, "/data_lake/products/2026-03-19/u/", now)
        
        # Get lineage snapshot
        snapshot = store.get_lineage_snapshot()
        assert len(snapshot) == 1, "Should have 1 schema entry"
        assert snapshot[0]["table_name"] == "products"
        assert len(snapshot[0]["output_partitions"]) == 2, "Should have 2 partitions"
        assert "/data_lake/products/2026-03-19/c/" in snapshot[0]["output_partitions"]
        assert "/data_lake/products/2026-03-19/u/" in snapshot[0]["output_partitions"]
        
        print("✓ Partition tracking and lineage snapshot")


def test_lineage_report_generation():
    """Test lineage report JSON generation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SchemaStore(f"{tmpdir}/state.db")
        now = utc_now_iso()
        
        # Register multiple tables with schema evolution
        schema_prod_v1 = {"id": "int", "description": "string", "price": "double"}
        sv_prod_v1 = store.get_or_register_schema("products", schema_prod_v1, now)
        store.track_partition("products", sv_prod_v1, "/data_lake/products/2026-03-19/c/", now)
        
        schema_cust_v1 = {"id": "int", "email": "string"}
        sv_cust_v1 = store.get_or_register_schema("customers", schema_cust_v1, now)
        store.track_partition("customers", sv_cust_v1, "/data_lake/customers/2026-03-19/c/", now)
        
        # Schema evolution for products
        schema_prod_v2 = {"id": "int", "product_description": "string", "price": "double"}
        sv_prod_v2 = store.get_or_register_schema("products", schema_prod_v2, now)
        store.track_partition("products", sv_prod_v2, "/data_lake/products/2026-03-20/c/", now)
        
        # Generate lineage
        report_path = f"{tmpdir}/lineage.json"
        
        from lineage_report import generate_lineage_report
        generate_lineage_report(store, report_path)
        
        # Verify report exists and is valid JSON
        assert Path(report_path).exists(), "Lineage report not created"
        report_text = Path(report_path).read_text()
        report = json.loads(report_text)
        
        assert isinstance(report, list), "Lineage report should be array"
        assert len(report) == 3, f"Should have 3 entries (2 products + 1 customers), got {len(report)}"
        
        # Find products entries
        products_entries = [r for r in report if "products" in r["source_table"]]
        assert len(products_entries) == 2, "Should have 2 products schema versions"
        
        # Find customers entries
        cust_entries = [r for r in report if "customers" in r["source_table"]]
        assert len(cust_entries) == 1, "Should have 1 customers schema version"
        
        # Verify required fields
        for entry in report:
            assert "source_table" in entry, "Missing source_table"
            assert "schema_version" in entry, "Missing schema_version"
            assert "active_from" in entry, "Missing active_from"
            assert "active_to" in entry, "Missing active_to"
            assert "transformation_logic" in entry, "Missing transformation_logic"
            assert "output_partitions" in entry, "Missing output_partitions"
            assert "output_schema" in entry, "Missing output_schema"
            
            # Verify types
            assert isinstance(entry["source_table"], str), "source_table should be string"
            assert isinstance(entry["schema_version"], int), "schema_version should be int"
            assert isinstance(entry["output_partitions"], list), "output_partitions should be array"
            assert isinstance(entry["output_schema"], dict), "output_schema should be object"
        
        print("✓ Lineage report generation")


def test_utc_now_iso():
    """Test ISO timestamp generation."""
    ts = utc_now_iso()
    
    # Should be valid ISO format ending with Z
    assert ts.endswith("Z"), f"Timestamp should end with Z: {ts}"
    
    # Should parse back to datetime
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    assert dt.tzinfo is not None, "Should be timezone-aware"
    
    # Should not have microseconds
    assert "." not in ts, f"Should not have microseconds: {ts}"
    
    print("✓ ISO timestamp generation")


def test_schema_store_persistence():
    """Test that schema store persists across reconnections."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/persistent.db"
        
        # Create store and register schema
        store1 = SchemaStore(db_path)
        now = utc_now_iso()
        schema = {"id": "int", "name": "string"}
        v1 = store1.get_or_register_schema("test", schema, now)
        assert v1 == 1
        
        # Create new store instance with same DB file
        store2 = SchemaStore(db_path)
        
        # Query the schema - should get the same version
        v1_again = store2.get_or_register_schema("test", schema, now)
        assert v1_again == 1, "Schema should persist across connections"
        
        # Add a new partition via first store, query via second
        store1.track_partition("test", 1, "/data_lake/test/2026-03-19/c/", now)
        snapshot = store2.get_lineage_snapshot()
        assert len(snapshot) == 1
        assert "/data_lake/test/2026-03-19/c/" in snapshot[0]["output_partitions"]
        
        print("✓ Schema store persistence")


def test_canonical_schema_normalization():
    """Test that schemas are canonicalized (order-independent)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SchemaStore(f"{tmpdir}/test.db")
        now = utc_now_iso()
        
        # Two schemas with same fields but different order in dict
        schema_a = {"id": "int", "name": "string", "price": "double"}
        schema_b = {"price": "double", "id": "int", "name": "string"}
        
        v_a = store.get_or_register_schema("items", schema_a, now)
        v_b = store.get_or_register_schema("items", schema_b, now)
        
        # Should be treated as same schema (same version)
        assert v_a == v_b == 1, f"Same schemas should have same version; got v_a={v_a}, v_b={v_b}"
        
        print("✓ Canonical schema normalization")


def test_null_schema_handling():
    """Test handling of null/None values in records."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = SchemaStore(f"{tmpdir}/test.db")
        now = utc_now_iso()
        
        # Schema with null/None values
        schema = {
            "id": "int",
            "optional_field": "null",
            "name": "string",
        }
        v = store.get_or_register_schema("items", schema, now)
        assert v == 1
        
        snapshot = store.get_lineage_snapshot()
        assert snapshot[0]["schema_definition"]["optional_field"] == "null"
        
        print("✓ Null schema handling")


def run_all_tests():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("RUNNING LOCAL UNIT TESTS (SCHEMA STORE & LINEAGE - NO DOCKER REQUIRED)")
    print("=" * 70 + "\n")
    
    tests = [
        test_schema_store_initialization,
        test_schema_registration,
        test_schema_per_table,
        test_partition_tracking,
        test_lineage_report_generation,
        test_utc_now_iso,
        test_schema_store_persistence,
        test_canonical_schema_normalization,
        test_null_schema_handling,
    ]
    
    passed = 0
    failed = 0
    errors = []
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            error_msg = f"{test.__name__}: {e}"
            print(f"✗ {error_msg}")
            errors.append(error_msg)
            failed += 1
        except Exception as e:
            error_msg = f"{test.__name__}: {type(e).__name__}: {e}"
            print(f"✗ {error_msg}")
            errors.append(error_msg)
            failed += 1
    
    print("\n" + "=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 70)
    
    if errors:
        print("\nFailed tests:")
        for error in errors:
            print(f"  - {error}")
    
    print()
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
