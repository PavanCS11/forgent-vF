"""
Procurement Data Pipeline Runner

Orchestrates the 6-step ETL pipeline:
1. Ingestion/Normalization - Maps raw data to golden schema
2. Enrichment - Joins with vendor/item master data
3. Maintenance - Refreshes mapping tables, AI enrichment (best-effort)
4. Assembly - Applies standardization, date filtering (2024+), and metrics
5. Power BI - Dashboard consumed from Parquet output
6. Analysis - Regression tests and validation

Reporting Period: 2024-01-01 onwards (2+ years, complete for both systems)
"""

import os
import sys
import glob
import traceback
from datetime import datetime

import pandas as pd

# Add project root to path so step_* imports resolve
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from step_1_ingestion.d_scripts.normalizer import Normalizer
from step_2_enrichment.a_scripts.pre_cleaner import PreCleaner
from step_3_maintenance.b_scripts.update_copper_prices import update_copper_prices
from step_3_maintenance.b_scripts.refresh_mappings import refresh_mappings
from step_3_maintenance.b_scripts.enrich_vendors import enrich_vendors
from step_3_maintenance.b_scripts.enrich_items import enrich_items
from step_3_maintenance.b_scripts.clean_item_map import clean_item_map
from step_3_maintenance.b_scripts.consolidate_items import consolidate_items
from step_4_assembly.a_scripts.final_builder import FinalBuilder


class PipelineError(Exception):
    """Raised when a critical pipeline phase fails."""
    def __init__(self, phase: str, message: str):
        self.phase = phase
        super().__init__(f"[{phase}] {message}")


def _count_rows(csv_path: str, step_name: str, expected_count: int = None,
                max_loss_pct: float = 5.0) -> int:
    """Count rows in a CSV and optionally validate against expected count.

    Args:
        csv_path: Path to the CSV file.
        step_name: Human-readable name for logging.
        expected_count: If provided, validate row loss against threshold.
        max_loss_pct: Maximum acceptable row loss percentage before failing.

    Returns:
        Actual row count.

    Raises:
        PipelineError: If row loss exceeds max_loss_pct.
    """
    actual = sum(1 for _ in open(csv_path, encoding='utf-8')) - 1  # subtract header
    print(f"  [{step_name}] Row count: {actual:,}")

    if expected_count is not None and expected_count > 0:
        diff = expected_count - actual
        if diff > 0:
            loss_pct = (diff / expected_count) * 100
            if loss_pct > max_loss_pct:
                raise PipelineError(step_name,
                    f"Excessive row loss: {diff:,} rows ({loss_pct:.1f}%) lost. "
                    f"Expected ~{expected_count:,}, got {actual:,}")
            else:
                print(f"  [{step_name}] Note: {diff:,} rows ({loss_pct:.1f}%) lost between steps")
    return actual


def _count_raw_input(input_dir: str, config_path: str) -> int:
    """Count total raw input rows across all source files."""
    raw_total = 0

    try:
        import yaml
        with open(config_path) as f:
            config = yaml.safe_load(f)
        raw_patterns = [
            (src['name'], os.path.join(input_dir, src['file_pattern']))
            for src in config.get('sources', [])
        ]
    except Exception as e:
        print(f"  Warning: Could not load source_mappings.yaml ({e}), using fallback patterns")
        raw_patterns = [
            ("Netsuite Open POs", os.path.join(input_dir, "Netsuite", "Netsuite Open POs*.csv")),
            ("Netsuite Receipts", os.path.join(input_dir, "Netsuite", "Netsuite Receipts*.csv")),
            ("Epicor Open POs (CSV)", os.path.join(input_dir, "Epicor", "Epicor Open POs*.csv")),
            ("Epicor Open POs (Excel)", os.path.join(input_dir, "Epicor", "Epicor Open POs*.xlsx")),
            ("Epicor Receipts (CSV)", os.path.join(input_dir, "Epicor", "Epicor Receipts*.csv")),
            ("Epicor Receipts (Excel)", os.path.join(input_dir, "Epicor", "Epicor Receipts*.xlsx")),
        ]

    for name, pattern in raw_patterns:
        files = glob.glob(pattern)
        source_total = 0
        for f in files:
            try:
                if f.lower().endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(f, usecols=[0])
                    source_total += len(df)
                else:
                    source_total += sum(1 for _ in open(f, encoding='utf-8-sig')) - 1
            except Exception as e:
                print(f"  ERROR reading {f}: {e}")
        print(f"  {name:25} | {source_total:,} rows")
        raw_total += source_total

    return raw_total


def print_completeness_check(input_dir: str, config_path: str,
                             count_normalized: int, count_staging: int, count_final: int):
    """Checks row completeness from raw input through to final output.

    Uses pre-computed row counts from pipeline checkpoints to avoid re-reading CSVs.
    Only raw input files are counted here (they aren't counted elsewhere).
    """
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETENESS CHECK")
    print("=" * 60)

    # Count raw input rows (the only count not already captured by _count_rows)
    print("\nRaw Input Files:")
    raw_total = _count_raw_input(input_dir, config_path)
    print(f"  {'TOTAL RAW INPUT':25} | {raw_total:,} rows")

    # Print stage counts from pipeline checkpoints
    print("\nPipeline Stage Counts:")
    print(f"  {'Normalized':25} | {count_normalized:,} rows")
    print(f"  {'Staging':25} | {count_staging:,} rows")
    print(f"  {'Final Output':25} | {count_final:,} rows")

    # Completeness analysis
    print("\nCompleteness Analysis:")
    stages = [
        ("Raw", "Normalized", raw_total, count_normalized),
        ("Normalized", "Staging", count_normalized, count_staging),
        ("Staging", "Final", count_staging, count_final),
    ]
    for from_name, to_name, from_count, to_count in stages:
        diff = from_count - to_count
        if diff != 0:
            pct = (diff / from_count) * 100 if from_count > 0 else 0
            print(f"  WARNING: {from_name} -> {to_name} lost {diff:,} rows ({pct:.2f}%)")
        else:
            print(f"  {from_name} -> {to_name}: OK (0 rows lost)")

    if raw_total == count_final:
        print(f"\n  PIPELINE COMPLETENESS: 100% - All {raw_total:,} rows preserved")
    else:
        total_lost = raw_total - count_final
        pct_preserved = (count_final / raw_total) * 100 if raw_total > 0 else 0
        print(f"\n  PIPELINE COMPLETENESS: {pct_preserved:.2f}% ({total_lost:,} rows lost)")

    print("\n" + "=" * 60)


def print_coverage_summary(final_csv_path: str):
    """Prints coverage summary with warnings for potential data gaps."""
    print("\n" + "=" * 60)
    print("DATA COVERAGE SUMMARY")
    print("=" * 60)

    try:
        df = pd.read_csv(final_csv_path, low_memory=False)
    except Exception as e:
        print(f"Could not load final output for coverage analysis: {e}")
        return

    # Row counts by source system and transaction type
    print("\nRow Counts by Source System & Transaction Type:")
    if 'source_system' in df.columns and 'transaction_type' in df.columns:
        coverage = df.groupby(['source_system', 'transaction_type']).size().reset_index(name='count')
        for _, row in coverage.iterrows():
            print(f"  {row['source_system']:12} | {row['transaction_type']:15} | {row['count']:,} rows")

        # Check for expected combinations that might be missing or low
        expected_combinations = [
            ('NETSUITE', 'Open Orders'),
            ('NETSUITE', 'Receipts'),
            ('EPICOR', 'Open Orders'),
            ('EPICOR', 'Receipts'),
        ]
        print("\nCoverage Check:")
        for source, txn_type in expected_combinations:
            match = coverage[(coverage['source_system'] == source) & (coverage['transaction_type'] == txn_type)]
            if match.empty:
                print(f"  WARNING: No data for {source} {txn_type}!")
            elif match['count'].values[0] < 100:
                print(f"  WARNING: Low row count for {source} {txn_type}: {match['count'].values[0]} rows")
    else:
        print("  Could not determine coverage (missing source_system or transaction_type columns)")

    # Date range summary
    print("\nDate Range Summary:")
    if 'order_date' in df.columns:
        df['order_date_parsed'] = pd.to_datetime(df['order_date'], errors='coerce')
        valid_dates = df['order_date_parsed'].dropna()

        if not valid_dates.empty:
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            days_span = (max_date - min_date).days
            days_since_newest = (datetime.now() - max_date).days

            print(f"  Earliest order:  {min_date.strftime('%Y-%m-%d')}")
            print(f"  Latest order:    {max_date.strftime('%Y-%m-%d')}")
            print(f"  Date span:       {days_span} days")

            if days_since_newest > 7:
                print(f"  WARNING: Most recent order is {days_since_newest} days old - data may be stale!")
            print(f"\n  NOTE: Final output filtered to orders >= 2024-01-01")
        else:
            print("  No valid order dates found")

    # Vendor coverage check for key vendors
    print("\nTop Vendors by Row Count:")
    if 'standardized_vendor_name' in df.columns:
        vendor_counts = df['standardized_vendor_name'].value_counts().head(10)
        for vendor, count in vendor_counts.items():
            print(f"  {vendor[:40]:40} | {count:,} rows")

    print("\n" + "=" * 60)


def main():
    """Main pipeline execution."""
    start_time = datetime.now()
    print("=" * 60)
    print("PROCUREMENT DATA PIPELINE")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # --- Path Configuration ---
    # Step 1: Ingestion Paths
    config_path = os.path.join(base_dir, "step_1_ingestion", "b.config", "source_mappings.yaml")
    schema_dir = os.path.join(base_dir, "step_1_ingestion", "c.schemas")
    input_dir = os.path.join(base_dir, "step_1_ingestion", "a.raw_data")
    normalized_dir = os.path.join(base_dir, "step_1_ingestion", "e.normalized")

    # Step 2: Enrichment Paths
    staging_dir = os.path.join(base_dir, "step_2_enrichment", "b.staging")

    # Step 3: Maintenance Paths
    vendor_map_path = os.path.join(base_dir, "step_3_maintenance", "a.mappings", "vendor_map.csv")
    item_map_path = os.path.join(base_dir, "step_3_maintenance", "a.mappings", "item_map.csv")

    # Step 4: Assembly Paths
    final_dir = os.path.join(base_dir, "step_4_assembly", "b.final")

    # Ensure output directories exist
    for d in [normalized_dir, staging_dir, final_dir]:
        os.makedirs(d, exist_ok=True)

    # --- Phase 1: Normalization (CRITICAL) ---
    print("\n" + "-" * 60)
    print("PHASE 1: NORMALIZATION")
    print("-" * 60)
    try:
        normalizer = Normalizer(config_path, schema_dir, input_dir, normalized_dir)
        normalizer.process_all()
    except Exception as e:
        raise PipelineError("NORMALIZATION", str(e)) from e
    print("Normalization Phase Complete.")

    # Row count checkpoint
    normalized_path = os.path.join(normalized_dir, "all_transactions_normalized.csv")
    count_after_normalize = _count_rows(normalized_path, "NORMALIZATION")

    # --- Phase 2: Pre-Enrichment (CRITICAL) ---
    print("\n" + "-" * 60)
    print("PHASE 2: ENRICHMENT")
    print("-" * 60)
    try:
        cleaner = PreCleaner(normalized_dir, staging_dir)
        cleaner.run()
    except Exception as e:
        raise PipelineError("ENRICHMENT", str(e)) from e
    print("Enrichment Phase Complete.")

    # Row count checkpoint (enrichment should not lose rows)
    staging_csv = os.path.join(staging_dir, "pre_cleaned_staging.csv")
    count_after_enrich = _count_rows(staging_csv, "ENRICHMENT", expected_count=count_after_normalize)

    # --- Phase 3: Maintenance (BEST-EFFORT) ---
    print("\n" + "-" * 60)
    print("PHASE 3: MAINTENANCE")
    print("-" * 60)

    maintenance_steps = [
        ("update_copper_prices", update_copper_prices),
        ("refresh_mappings", refresh_mappings),
        ("enrich_vendors", enrich_vendors),
        ("enrich_items", enrich_items),
        ("clean_item_map", clean_item_map),
        ("consolidate_items", consolidate_items),
    ]

    maintenance_errors = []
    for step_name, step_func in maintenance_steps:
        try:
            step_func()
        except Exception as e:
            print(f"\n  WARNING: {step_name} failed: {e}")
            traceback.print_exc()
            maintenance_errors.append(step_name)

    if maintenance_errors:
        print(f"\n  WARNING: {len(maintenance_errors)} maintenance step(s) failed: {maintenance_errors}")
        print("  Continuing with assembly using existing mapping data.")

    print("Maintenance Phase Complete.")

    # --- Phase 4: Final Assembly (CRITICAL) ---
    print("\n" + "-" * 60)
    print("PHASE 4: FINAL ASSEMBLY")
    print("-" * 60)
    metadata_path = os.path.join(normalized_dir, "pipeline_metadata.json")
    try:
        builder = FinalBuilder(
            staging_path=os.path.join(staging_dir, "pre_cleaned_staging.csv"),
            vendor_map_path=vendor_map_path,
            item_map_path=item_map_path,
            output_dir=final_dir,
            metadata_path=metadata_path
        )
        builder.run()
    except Exception as e:
        raise PipelineError("ASSEMBLY", str(e)) from e
    print("Assembly Phase Complete.")

    # Row count checkpoint (assembly applies date filter, so use lenient threshold)
    final_csv_path = os.path.join(final_dir, "po_fact_table.csv")
    count_after_assembly = _count_rows(final_csv_path, "ASSEMBLY", expected_count=count_after_enrich, max_loss_pct=50.0)

    # --- Coverage Summary ---
    print_coverage_summary(final_csv_path)

    # --- Completeness Check ---
    print_completeness_check(
        input_dir=input_dir,
        config_path=config_path,
        count_normalized=count_after_normalize,
        count_staging=count_after_enrich,
        count_final=count_after_assembly,
    )

    # --- Regression Tests ---
    try:
        from step_6_analysis.validate_output import validate_output, print_results
        results = validate_output(final_csv_path)
        failures = print_results(results)
        if failures > 0:
            print(f"\n  WARNING: {failures} regression test(s) failed!")
    except ImportError:
        print("\n  Note: Validation module not available (step_6_analysis)")

    # --- Final Summary ---
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    if maintenance_errors:
        print(f"PIPELINE COMPLETE (with {len(maintenance_errors)} maintenance warning(s))")
    else:
        print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"\nOutputs saved to: {final_dir}")
    print("  - po_fact_table.csv")
    print("  - po_fact_table.parquet")
    print("=" * 60)


if __name__ == "__main__":
    main()
