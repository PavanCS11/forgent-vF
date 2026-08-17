"""
Data Quality Module for PO Fact Table
======================================

Provides data quality checks with configurable severity levels.
Critical checks will fail the pipeline; warnings are logged but don't block.

Usage:
    checker = DataQualityChecker(df)
    results = checker.run_all_checks()
    if not checker.passed:
        raise DataQualityError(checker.get_critical_failures())
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from enum import Enum


class Severity(Enum):
    """Severity levels for data quality checks."""
    CRITICAL = "CRITICAL"  # Fails pipeline
    WARNING = "WARNING"    # Logged but continues


@dataclass
class DQCheckResult:
    """Result of a single data quality check."""
    check_name: str
    severity: Severity
    passed: bool
    message: str
    row_count: int
    sample_keys: List[str] = field(default_factory=list)

    @property
    def status_str(self) -> str:
        return "PASS" if self.passed else f"FAIL ({self.severity.value})"


class DataQualityError(Exception):
    """Raised when critical data quality checks fail."""
    pass


class DataQualityChecker:
    """
    Runs data quality checks on the PO fact table.

    Critical checks (fail pipeline):
    - Required keys must not be null (po_number, po_line)

    Warning checks (logged but continue):
    - Negative quantities
    - Status consistency (open_amount vs is_open_order)
    - Date order violations (receipt before order)
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.results: List[DQCheckResult] = []
        self._passed = True

    @property
    def passed(self) -> bool:
        """Returns True if all CRITICAL checks passed."""
        return self._passed

    def run_all_checks(self) -> List[DQCheckResult]:
        """Run all data quality checks and return results."""
        self.results = []

        # Critical checks
        self._check_required_keys()

        # Warning checks
        self._check_negative_quantities()
        self._check_status_consistency()
        self._check_date_order_violations()
        self._check_grain_uniqueness()
        self._check_quantity_tieout()
        self._check_late_overdue_consistency()
        self._check_otd_flag_values()

        # New spend-related checks
        self._check_transaction_type_overlap()
        self._check_high_unit_cost()
        self._check_unit_cost_corrections()
        self._check_reporting_date_coverage()

        return self.results

    def _check_required_keys(self):
        """
        CRITICAL: Check that po_number is never null.
        po_line is only required for Open Orders — receipt exports (NS and Epicor)
        do not include a PO line column, so receipts legitimately have null po_line.
        """
        missing_po = self.df['po_number'].isna() | (self.df['po_number'].astype(str).str.strip() == '')

        # po_line required only for Open Orders (receipts structurally lack this field)
        is_open_order = self.df.get('transaction_type', pd.Series(dtype=str)) == 'Open Orders'
        missing_line_open = (
            is_open_order &
            (self.df['po_line'].isna() | (self.df['po_line'].astype(str).str.strip() == ''))
        )

        missing_count = (missing_po | missing_line_open).sum()
        passed = missing_count == 0

        if not passed:
            self._passed = False
            bad_rows = self.df[missing_po | missing_line_open].head(5)
            sample_keys = [
                f"row {idx}: po_number='{row.get('po_number', '')}', po_line='{row.get('po_line', '')}'"
                for idx, row in bad_rows.iterrows()
            ]
        else:
            sample_keys = []

        self.results.append(DQCheckResult(
            check_name="Required Keys (po_number; po_line for Open Orders)",
            severity=Severity.CRITICAL,
            passed=passed,
            message=f"Found {missing_count} rows with missing po_number or (Open Orders) missing po_line",
            row_count=missing_count,
            sample_keys=sample_keys
        ))

    def _check_negative_quantities(self):
        """
        WARNING: Detect rows with negative quantities.
        These may indicate data issues or reversals.
        """
        qty_cols = ['total_quantity', 'received_quantity', 'open_quantity']
        negative_count = 0
        details = []

        for col in qty_cols:
            if col in self.df.columns:
                # Convert to numeric if needed
                numeric_col = pd.to_numeric(self.df[col], errors='coerce')
                neg_mask = numeric_col < 0
                neg = neg_mask.sum()
                if neg > 0:
                    negative_count += neg
                    details.append(f"{col}: {neg}")

        passed = negative_count == 0

        self.results.append(DQCheckResult(
            check_name="Negative Quantities",
            severity=Severity.WARNING,
            passed=passed,
            message=f"Found {negative_count} rows with negative quantities ({'; '.join(details) if details else 'none'})",
            row_count=negative_count
        ))

    def _check_status_consistency(self):
        """
        WARNING: Check open_amount vs is_open_order consistency.
        Rows with open_amount > 0 should typically have is_open_order = True.
        """
        if 'open_amount' not in self.df.columns or 'is_open_order' not in self.df.columns:
            self.results.append(DQCheckResult(
                check_name="Status Consistency",
                severity=Severity.WARNING,
                passed=True,
                message="Skipped - required columns not present",
                row_count=0
            ))
            return

        # Convert to numeric/bool
        open_amount = pd.to_numeric(self.df['open_amount'], errors='coerce').fillna(0)

        # Handle various boolean representations
        is_open = self.df['is_open_order']
        if is_open.dtype == 'object':
            is_open = is_open.astype(str).str.lower().isin(['true', '1', 'yes'])
        else:
            is_open = is_open.fillna(False).astype(bool)

        # Inconsistent: has open amount but marked as closed
        inconsistent = (open_amount > 0) & (~is_open)
        inconsistent_count = inconsistent.sum()

        passed = inconsistent_count == 0

        self.results.append(DQCheckResult(
            check_name="Status Consistency (open_amount vs is_open_order)",
            severity=Severity.WARNING,
            passed=passed,
            message=f"Found {inconsistent_count} rows with open_amount > 0 but is_open_order = False",
            row_count=inconsistent_count
        ))

    def _check_date_order_violations(self):
        """
        WARNING: Check for receipt_date before order_date.
        This is usually a data entry error.
        """
        if 'receipt_date' not in self.df.columns or 'order_date' not in self.df.columns:
            self.results.append(DQCheckResult(
                check_name="Date Order Violations",
                severity=Severity.WARNING,
                passed=True,
                message="Skipped - required columns not present",
                row_count=0
            ))
            return

        receipt_date = pd.to_datetime(self.df['receipt_date'], errors='coerce')
        order_date = pd.to_datetime(self.df['order_date'], errors='coerce')

        # Violation: receipt before order (both must be present)
        violations = (
            receipt_date.notna() &
            order_date.notna() &
            (receipt_date < order_date)
        )
        violation_count = violations.sum()

        passed = violation_count == 0

        # Get sample
        sample_keys = []
        if not passed:
            bad_rows = self.df[violations].head(5)
            sample_keys = [
                f"PO {row.get('po_number')}: order={row.get('order_date')}, receipt={row.get('receipt_date')}"
                for _, row in bad_rows.iterrows()
            ]

        self.results.append(DQCheckResult(
            check_name="Date Order Violations (receipt < order)",
            severity=Severity.WARNING,
            passed=passed,
            message=f"Found {violation_count} rows where receipt_date < order_date",
            row_count=violation_count,
            sample_keys=sample_keys
        ))

    def _check_grain_uniqueness(self):
        """
        WARNING: Check that primary key columns form unique rows.
        Duplicates can cause double-counting in analytics.
        """
        try:
            from .grain_definition import validate_grain_uniqueness
        except ImportError:
            from grain_definition import validate_grain_uniqueness

        is_valid, duplicate_count, sample_df = validate_grain_uniqueness(self.df)

        sample_keys = []
        if not is_valid and sample_df is not None:
            sample_keys = [
                f"PO {row.get('po_number')}/{row.get('po_line')}"
                for _, row in sample_df.head(5).iterrows()
            ]

        self.results.append(DQCheckResult(
            check_name="Grain Uniqueness",
            severity=Severity.WARNING,
            passed=is_valid,
            message=f"Found {duplicate_count} duplicate rows by primary key" if not is_valid else "All rows unique",
            row_count=duplicate_count,
            sample_keys=sample_keys
        ))

    def _check_quantity_tieout(self):
        """
        WARNING: For OPEN_ORDER transactions, verify that open_qty + received_qty = total_quantity.
        This validates the core equation of the 6-column quantity/amount structure.
        """
        required_cols = ['open_quantity', 'received_quantity', 'total_quantity', 'transaction_type']
        if not all(col in self.df.columns for col in required_cols):
            self.results.append(DQCheckResult(
                check_name="Quantity Tie-out (open + received = total)",
                severity=Severity.WARNING,
                passed=True,
                message="Skipped - required columns not present",
                row_count=0
            ))
            return

        # Only check Open Orders rows (receipts have open_quantity=0 by design)
        is_open_order = self.df['transaction_type'] == 'Open Orders'
        open_orders = self.df[is_open_order].copy()

        if len(open_orders) == 0:
            self.results.append(DQCheckResult(
                check_name="Quantity Tie-out (open + received = total)",
                severity=Severity.WARNING,
                passed=True,
                message="No Open Orders rows to check",
                row_count=0
            ))
            return

        # Calculate: open + received should equal total
        open_qty = pd.to_numeric(open_orders['open_quantity'], errors='coerce').fillna(0)
        received_qty = pd.to_numeric(open_orders['received_quantity'], errors='coerce').fillna(0)
        total_qty = pd.to_numeric(open_orders['total_quantity'], errors='coerce').fillna(0)

        # Allow small tolerance for floating point (0.01)
        difference = abs((open_qty + received_qty) - total_qty)
        violations = difference > 0.01
        violation_count = violations.sum()

        passed = violation_count == 0

        # Get sample of violations
        sample_keys = []
        if not passed:
            bad_rows = open_orders[violations].head(5)
            for _, row in bad_rows.iterrows():
                o = row.get('open_quantity', 0)
                r = row.get('received_quantity', 0)
                t = row.get('total_quantity', 0)
                sample_keys.append(
                    f"PO {row.get('po_number')}/{row.get('po_line')}: open={o} + recv={r} = {o+r} != total={t}"
                )

        self.results.append(DQCheckResult(
            check_name="Quantity Tie-out (open + received = total)",
            severity=Severity.WARNING,
            passed=passed,
            message=f"Found {violation_count} Open Orders rows where open + received != total" if not passed else "All Open Orders rows pass tie-out",
            row_count=violation_count,
            sample_keys=sample_keys
        ))

    def _check_late_overdue_consistency(self):
        """
        WARNING: Validates is_late_or_overdue includes all is_overdue cases.
        Ensures the unified flag correctly captures both late and overdue scenarios.
        """
        if 'is_late_or_overdue' not in self.df.columns or 'is_overdue' not in self.df.columns:
            return

        # If is_overdue=True, then is_late_or_overdue must also be True
        inconsistent = (self.df['is_overdue'] == True) & (self.df['is_late_or_overdue'] == False)

        if inconsistent.any():
            sample_pos = self.df.loc[inconsistent, 'po_number'].head(5).tolist()
            self.results.append(DQCheckResult(
                check_name="late_overdue_flag_consistency",
                severity=Severity.WARNING,
                passed=False,
                message=f"Found {inconsistent.sum():,} rows where is_overdue=True but is_late_or_overdue=False",
                row_count=inconsistent.sum(),
                sample_keys=sample_pos
            ))
        else:
            self.results.append(DQCheckResult(
                check_name="late_overdue_flag_consistency",
                severity=Severity.WARNING,
                passed=True,
                message="is_late_or_overdue correctly includes all is_overdue cases",
                row_count=0
            ))

    def _check_otd_flag_values(self):
        """
        WARNING: Validates OTD flag columns contain only valid 3-category values.
        Ensures on_time_vs_due_flag and on_time_vs_promise_flag contain only
        'Early', 'On Time', 'Late', or null.
        """
        valid_values = {'Early', 'On Time', 'Late'}

        for col in ['on_time_vs_due_flag', 'on_time_vs_promise_flag']:
            if col not in self.df.columns:
                continue

            actual_values = set(self.df[col].dropna().unique())
            invalid_values = actual_values - valid_values

            if invalid_values:
                self.results.append(DQCheckResult(
                    check_name=f"otd_flag_valid_values_{col}",
                    severity=Severity.WARNING,
                    passed=False,
                    message=f"{col} contains invalid values: {invalid_values}. Expected: {valid_values}",
                    row_count=self.df[col].isin(list(invalid_values)).sum()
                ))
            else:
                self.results.append(DQCheckResult(
                    check_name=f"otd_flag_valid_values_{col}",
                    severity=Severity.WARNING,
                    passed=True,
                    message=f"{col} contains only valid OTD categories",
                    row_count=0
                ))

    def _check_transaction_type_overlap(self):
        """
        WARNING: Check for PO lines that have both OPEN_ORDER and RECEIPT rows.
        This could lead to double-counting if not handled properly in analytics.
        """
        required_cols = ['source_system', 'po_number', 'po_line', 'transaction_type']
        if not all(col in self.df.columns for col in required_cols):
            self.results.append(DQCheckResult(
                check_name="Transaction Type Overlap",
                severity=Severity.WARNING,
                passed=True,
                message="Skipped - required columns not present",
                row_count=0
            ))
            return

        # Group by PO line and count distinct transaction types
        po_line_types = self.df.groupby(['source_system', 'po_number', 'po_line'])['transaction_type'].nunique()
        overlap_count = (po_line_types > 1).sum()

        passed = overlap_count == 0

        sample_keys = []
        if not passed:
            # Get sample of overlapping PO lines
            overlapping = po_line_types[po_line_types > 1].head(5).index.tolist()
            for source, po, line in overlapping:
                types = self.df[(self.df['source_system'] == source) &
                               (self.df['po_number'] == po) &
                               (self.df['po_line'] == line)]['transaction_type'].unique()
                sample_keys.append(f"{source} PO {po}/{line}: {list(types)}")

        # Epicor partially-received POs legitimately appear in both Open Orders and Receipts
        # (different transaction types, not double-counting). Keep as WARNING only.
        self.results.append(DQCheckResult(
            check_name="Transaction Type Overlap (Double-Count Risk)",
            severity=Severity.WARNING,
            passed=passed,
            message=f"Found {overlap_count} PO lines with both OPEN_ORDER and RECEIPT rows - potential double-counting risk" if not passed else "No overlap detected",
            row_count=overlap_count,
            sample_keys=sample_keys
        ))

    def _check_high_unit_cost(self):
        """
        WARNING: Check for suspiciously high unit_cost values.
        Values > $50,000 may indicate uncorrected 1000x issues or data entry errors.
        """
        if 'unit_cost' not in self.df.columns:
            self.results.append(DQCheckResult(
                check_name="High Unit Cost Check",
                severity=Severity.WARNING,
                passed=True,
                message="Skipped - unit_cost column not present",
                row_count=0
            ))
            return

        threshold = 50000  # $50K threshold
        unit_cost = pd.to_numeric(self.df['unit_cost'], errors='coerce')
        high_cost = unit_cost > threshold
        high_cost_count = high_cost.sum()

        passed = high_cost_count == 0

        sample_keys = []
        if not passed:
            high_rows = self.df[high_cost].head(5)
            for _, row in high_rows.iterrows():
                sample_keys.append(
                    f"PO {row.get('po_number')}/{row.get('po_line')}: unit_cost=${row.get('unit_cost'):,.2f}"
                )

        self.results.append(DQCheckResult(
            check_name="High Unit Cost (>$50K)",
            severity=Severity.WARNING,
            passed=passed,
            message=f"Found {high_cost_count} rows with unit_cost > ${threshold:,} - verify these are legitimate" if not passed else "No suspiciously high unit costs",
            row_count=high_cost_count,
            sample_keys=sample_keys
        ))

    def _check_unit_cost_corrections(self):
        """
        INFO: Report on unit_cost corrections that were applied.
        This is informational, not a failure condition.
        """
        if '_unit_cost_correction' not in self.df.columns:
            return  # Column not present, skip

        corrections = self.df['_unit_cost_correction'].notna()
        correction_count = corrections.sum()

        if correction_count > 0:
            # Calculate total dollar impact
            if '_original_unit_cost' in self.df.columns and 'calculated_price' in self.df.columns:
                orig = pd.to_numeric(self.df['_original_unit_cost'], errors='coerce')
                corrected = pd.to_numeric(self.df['calculated_price'], errors='coerce')
                qty = pd.to_numeric(self.df.get('total_quantity', 0), errors='coerce').fillna(0)
                impact = abs((orig - corrected) * qty)
                total_impact = impact[corrections].sum()
                message = f"Applied {correction_count} unit_cost corrections (1000x mills-to-dollars). Dollar impact: ${total_impact:,.2f}"
            else:
                message = f"Applied {correction_count} unit_cost corrections (1000x mills-to-dollars)"

            sample_keys = []
            corrected_rows = self.df[corrections].head(3)
            for _, row in corrected_rows.iterrows():
                sample_keys.append(
                    f"PO {row.get('po_number')}: ${row.get('_original_unit_cost'):,.2f} -> ${row.get('calculated_price'):,.2f}"
                )

            self.results.append(DQCheckResult(
                check_name="Unit Cost Corrections Applied",
                severity=Severity.WARNING,  # Informational but tracked
                passed=True,  # Corrections are expected, not a failure
                message=message,
                row_count=correction_count,
                sample_keys=sample_keys
            ))

    def _check_reporting_date_coverage(self):
        """
        WARNING: Verify both source systems have adequate coverage after date filtering.
        Ensures no system is unfairly represented in reporting period.
        """
        reporting_start = pd.to_datetime('2024-01-01')

        if 'order_date' not in self.df.columns or 'source_system' not in self.df.columns:
            self.results.append(DQCheckResult(
                check_name="Reporting Date Coverage",
                severity=Severity.WARNING,
                passed=False,
                message="Cannot check - missing order_date or source_system columns",
                row_count=0
            ))
            return

        coverage_issues = []
        for source in ['NETSUITE', 'EPICOR']:
            source_data = self.df[self.df['source_system'] == source]
            if source_data.empty:
                coverage_issues.append(f"{source}: No data")
                continue

            min_date = source_data['order_date'].min()

            if pd.notna(min_date) and min_date > reporting_start:
                days_gap = (min_date - reporting_start).days
                coverage_issues.append(
                    f"{source}: Data starts {days_gap} days after 2024-01-01 ({min_date.strftime('%Y-%m-%d')})"
                )

        passed = len(coverage_issues) == 0

        self.results.append(DQCheckResult(
            check_name="Reporting Date Coverage",
            severity=Severity.WARNING,
            passed=passed,
            message="; ".join(coverage_issues) if coverage_issues else "Both systems have coverage from 2024-01-01",
            row_count=len(coverage_issues),
            sample_keys=coverage_issues
        ))

    def get_critical_failures(self) -> List[DQCheckResult]:
        """Returns list of failed CRITICAL checks."""
        return [r for r in self.results if r.severity == Severity.CRITICAL and not r.passed]

    def get_warnings(self) -> List[DQCheckResult]:
        """Returns list of failed WARNING checks."""
        return [r for r in self.results if r.severity == Severity.WARNING and not r.passed]

    def generate_report(self) -> str:
        """Generate a formatted DQ report."""
        lines = []
        lines.append("=" * 70)
        lines.append("DATA QUALITY REPORT")
        lines.append(f"Generated: {datetime.now().isoformat()}")
        lines.append(f"Total Rows Checked: {len(self.df):,}")
        lines.append("=" * 70)

        # Group by severity
        critical_results = [r for r in self.results if r.severity == Severity.CRITICAL]
        warning_results = [r for r in self.results if r.severity == Severity.WARNING]

        # Critical section
        lines.append("\n--- CRITICAL CHECKS ---")
        for result in critical_results:
            lines.append(f"\n[{result.status_str}] {result.check_name}")
            lines.append(f"    {result.message}")
            if result.sample_keys:
                lines.append("    Samples:")
                for key in result.sample_keys[:3]:
                    lines.append(f"      - {key}")

        # Warning section
        lines.append("\n--- WARNING CHECKS ---")
        for result in warning_results:
            lines.append(f"\n[{result.status_str}] {result.check_name}")
            lines.append(f"    {result.message}")
            if result.sample_keys:
                lines.append("    Samples:")
                for key in result.sample_keys[:3]:
                    lines.append(f"      - {key}")

        # Summary
        lines.append("\n" + "=" * 70)
        critical_failed = len([r for r in critical_results if not r.passed])
        warning_failed = len([r for r in warning_results if not r.passed])

        if critical_failed > 0:
            lines.append(f"RESULT: FAILED - {critical_failed} critical issue(s)")
        elif warning_failed > 0:
            lines.append(f"RESULT: PASSED WITH WARNINGS - {warning_failed} warning(s)")
        else:
            lines.append("RESULT: PASSED - All checks passed")

        lines.append("=" * 70)

        return "\n".join(lines)

    def save_report(self, output_path: str):
        """Save the DQ report to a file."""
        report = self.generate_report()
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)
