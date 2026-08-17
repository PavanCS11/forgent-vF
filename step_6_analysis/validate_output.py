"""
Output Validation - Regression Tests
=====================================

Run after pipeline completes to verify the output fact table meets expectations.
Can be run standalone or called from pipeline_runner.py.

Usage:
    python -m step_6_analysis.validate_output [--fact-table path/to/po_fact_table.csv]
"""

import os
import sys
import argparse

import pandas as pd


DEFAULT_FACT_TABLE = os.path.join(
    os.path.dirname(__file__), '..', 'step_4_assembly', 'b.final', 'po_fact_table.csv'
)


class ValidationResult:
    def __init__(self, name: str, passed: bool, message: str):
        self.name = name
        self.passed = passed
        self.message = message


def validate_output(fact_table_path: str = None) -> list:
    """Run regression tests on the pipeline output.

    Returns:
        List of ValidationResult objects.
    """
    path = fact_table_path or DEFAULT_FACT_TABLE

    if not os.path.exists(path):
        return [ValidationResult("File Exists", False, f"Not found: {path}")]

    df = pd.read_csv(path, low_memory=False)
    results = []

    # Test 1: Minimum row count (baseline from current known output)
    results.append(ValidationResult(
        "Minimum Row Count",
        len(df) >= 50000,
        f"Got {len(df):,} rows (minimum: 50,000)"
    ))

    # Test 2: Both source systems present
    sources = set(df['source_system'].unique()) if 'source_system' in df.columns else set()
    results.append(ValidationResult(
        "Both Sources Present",
        {'NETSUITE', 'EPICOR'} <= sources,
        f"Sources found: {sources}"
    ))

    # Test 3: Both transaction types present
    types = set(df['transaction_type'].unique()) if 'transaction_type' in df.columns else set()
    results.append(ValidationResult(
        "Both Transaction Types",
        {'Open Orders', 'Receipts'} <= types,
        f"Types found: {types}"
    ))

    # Test 4: Critical columns have low null rates
    critical_cols = ['po_number', 'vendor_name', 'item_id', 'order_date']
    for col in critical_cols:
        if col in df.columns:
            null_pct = df[col].isna().sum() / len(df) * 100
            results.append(ValidationResult(
                f"Column Coverage: {col}",
                null_pct < 10,
                f"{null_pct:.1f}% null (threshold: <10%)"
            ))

    # Test 5: Vendor match rate
    if 'standardized_vendor_name' in df.columns:
        vendor_filled = df['standardized_vendor_name'].notna() & (df['standardized_vendor_name'] != '')
        vendor_rate = vendor_filled.sum() / len(df) * 100
        results.append(ValidationResult(
            "Vendor Match Rate",
            vendor_rate > 60,
            f"{vendor_rate:.1f}% matched (threshold: >60%)"
        ))

    # Test 6: Item categorization rate
    if 'item_category_l1' in df.columns:
        categorized = (df['item_category_l1'] != 'Uncategorized') & df['item_category_l1'].notna()
        cat_rate = categorized.sum() / len(df) * 100
        results.append(ValidationResult(
            "Item Categorization Rate",
            cat_rate > 20,
            f"{cat_rate:.1f}% categorized (threshold: >20%)"
        ))

    # Test 7: Dollar amounts are reasonable
    if 'open_plus_received_amount' in df.columns:
        total_spend = df['open_plus_received_amount'].sum()
        results.append(ValidationResult(
            "Total Spend Positive",
            total_spend > 0,
            f"Total spend: ${total_spend:,.2f}"
        ))

    # Test 8: No completely empty source system groups
    if 'source_system' in df.columns and 'transaction_type' in df.columns:
        combos = df.groupby(['source_system', 'transaction_type']).size()
        min_count = combos.min()
        results.append(ValidationResult(
            "All Source/Type Combos Non-Empty",
            min_count >= 10,
            f"Smallest group has {min_count:,} rows"
        ))

    return results


def print_results(results: list) -> int:
    """Print validation results and return count of failures."""
    print("\n" + "=" * 60)
    print("OUTPUT VALIDATION - Regression Tests")
    print("=" * 60)

    passed = 0
    failed = 0
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.name}: {r.message}")
        if r.passed:
            passed += 1
        else:
            failed += 1

    print(f"\n  Results: {passed} passed, {failed} failed")
    print("=" * 60)
    return failed


def main():
    parser = argparse.ArgumentParser(description="Validate pipeline output")
    parser.add_argument("--fact-table", default=None, help="Path to po_fact_table.csv")
    args = parser.parse_args()

    results = validate_output(args.fact_table)
    failures = print_results(results)
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
