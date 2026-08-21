"""
Schema Contract for PO Fact Table
=================================

Defines the explicit pyarrow schema for the po_fact_table.parquet output.
This ensures Power BI can load the file with correct types without any
Table.TransformColumnTypes in Power Query.

Schema Version: 2.13.0

Version 2.12.0 Changes (2026-07-14):
- Added item_category_l4 and item_category_l5 to carry VanTran taxonomy levels 4 and 5
- Existing systems continue to populate levels 1-3 only; levels 4-5 remain null for NetSuite and Epicor

Version 2.11.0 Changes (2026-03-11):
- Removed expected_receipt_date: replaced by line-level join in ingestion phase
- Added old_promise_date: NS Receipts only; the pre-join PO Promise Date for audit/reference
- promise_date for NS Receipts now sourced from PO Line Dates join (Maximum of PO New Promise Date)
- OTD effective_promise chain simplified: promise_date > due_date (no more expected_receipt_date fallback)
- analysis_date open-order chain simplified: due_date > promise_date > order_date

Version 2.10.0 Changes (2026-03-09):
- OTD Policy Update: on_time_flag now aliases on_time_vs_promise_flag (was on_time_vs_due_flag)
- Primary OTD metric uses contractual promise date: promise_date > expected_receipt_date
- due_date excluded from OTD scoring — it is an operational/MRP date that changes throughout PO life
- is_overdue now uses promise-based date chain: promise_date > expected_receipt_date > due_date
- on_time_vs_promise_flag uses effective promise (promise_date > expected_receipt_date)
- on_time_vs_due_flag unchanged (still uses due_date fallback chain, for operational tracking)
- Grace period unchanged: 14 days early, 3 days late

Version 2.9.0 Changes (2026-02-25):
- Added payment terms standardization via terms_map.csv
- Unifies cross-system terms: N30/Net 30 → Net 30, N45/Net 45 → Net 45, etc.
- Immediate-payment terms (COD, CIA, Cash, Credit Card, CARD, Due on Receipt) → Due on Receipt
- Epicor discount codes (D01-D10, D99) → Discount Terms
- Reduces terms_code from ~30 unique values to 9 standardized buckets

Version 2.8.0 Changes (2026-02-19):
- Added item_canonical_name: Best descriptive name for semantic item groups
- Added item_canonical_group_id: Surrogate key for grouping identical items
- Enables PowerBI to aggregate spend by canonical item instead of raw Item_ID
- 3-pass consolidation: exact match, spec fingerprint, AI fuzzy match

Version 2.7.0 Changes (2026-02-16):
- Added item_name: Unified best-available descriptive name for items
- Priority chain: item_display_name > item_description > item_raw_description > line_description > item_name_mpn
- item_short_name now derived from item_name (was item_name_mpn)

Version 2.6.0 Changes (2026-02-10):
- Changed on_time_flag, on_time_vs_due_flag, on_time_vs_promise_flag from bool to string
- 3-category OTD classification: "Early", "On Time", "Late"
- Early: receipt_date < (due_date - 14 days)
- On Time: (due_date - 14 days) <= receipt_date <= (due_date + 3 days)
- Late: receipt_date > (due_date + 3 days)
- is_late_or_overdue now checks on_time_flag='Late' instead of =FALSE

Version 2.5.0 Changes (2026-02-08):
- Added is_late_or_overdue: Unified flag combining historical late deliveries and current overdue orders
- TRUE when either on_time_flag='Late' (received late) OR is_overdue=TRUE (currently overdue)
- Simplifies Power BI filtering for all late/overdue scenarios

Version 2.4.0 Changes (2026-02-08):
- Added REPORTING_START_DATE filter (2024-01-01)
- Ensures both NetSuite and Epicor have complete calendar year coverage
- Filters applied in final_builder.py before metrics calculation
- Uses inclusive OR logic: (order_date >= 2024-01-01) OR (receipt_date >= 2024-01-01)
- Supports dual reporting perspectives (order date view AND receipt date view)

Version 2.3.0 Changes (2026-02-08):
- Added 3-day grace period to all late delivery calculations
- is_overdue: Flagged only if >3 days past due_date
- on_time_vs_due_flag: "On Time" if received within (due_date - 14 days) to (due_date + 3 days)
- on_time_vs_promise_flag: "On Time" if final completion receipt <= promise_date + 3 days; "Late" after that; NULL while still open and within grace
- days_early_late_vs_due/promise: Adjusted to subtract 3 days when late
"""

import pyarrow as pa
from datetime import datetime

# Schema version - increment when making breaking changes
SCHEMA_VERSION = "2.13.0"  # PO-line OTD logic: no Early, final receipt, representative row


def get_po_fact_schema() -> pa.Schema:
    """
    Returns the pyarrow schema for the PO fact table.

    Type Categories:
    - Strings: All IDs, codes, names, descriptions, categories, spec text
    - Int64: po_line, vendor_id, item_id, days_since_order
    - Int32: order_year, order_month (calendar attributes)
    - Float64: All quantities, money, variance fields
    - Boolean: is_open_order, is_overdue, is_fully_received, analysis_date_is_fallback
    - Date32: All date fields
    - Timestamp: _load_timestamp (metadata)
    """

    return pa.schema([
        # ============================================
        # SYSTEM & TYPE IDENTIFIERS (string)
        # ============================================
        ('source_system', pa.string()),
        ('transaction_type', pa.string()),

        # ============================================
        # PO IDENTIFIERS (string)
        # ============================================
        ('po_number', pa.string()),
        ('po_line', pa.string()),  # Keep as string for consistency
        ('po_release_num', pa.string()),

        # ============================================
        # RECEIPT IDENTIFIERS (string)
        # ============================================
        ('receipt_number', pa.string()),
        ('receipt_line', pa.string()),

        # ============================================
        # ORGANIZATION (string)
        # ============================================
        ('subsidiary_company', pa.string()),
        ('department', pa.string()),
        ('location_warehouse', pa.string()),
        ('plant_site', pa.string()),

        # ============================================
        # PEOPLE (string)
        # ============================================
        ('created_by', pa.string()),
        ('buyer_id', pa.string()),

        # ============================================
        # STATUS FLAGS
        # ============================================
        ('po_status', pa.string()),
        ('is_open_order', pa.bool_()),
        ('is_open_release', pa.bool_()),
        ('is_confirmed', pa.bool_()),
        ('bill_variance_status', pa.string()),

        # ============================================
        # DATE FIELDS (date32 - proper nulls, not empty strings)
        # ============================================
        ('order_date', pa.date32()),
        ('due_date', pa.date32()),
        ('promise_date', pa.date32()),
        ('old_promise_date', pa.date32()),
        ('requested_date', pa.date32()),
        ('receipt_date', pa.date32()),
        ('arrived_date', pa.date32()),

        # ============================================
        # QUANTITY FIELDS (float64 - NOT text)
        # ============================================
        ('total_quantity', pa.float64()),
        ('received_quantity', pa.float64()),
        ('open_quantity', pa.float64()),
        ('vendor_quantity', pa.float64()),           # Was text, now numeric
        ('uninvoiced_qty_supplier', pa.float64()),   # Was text, now numeric
        ('uninvoiced_qty_internal', pa.float64()),   # Was text, now numeric

        # ============================================
        # FINANCIAL FIELDS (float64)
        # ============================================
        ('unit_cost', pa.float64()),
        ('cost_per_code', pa.string()),

        # ============================================
        # SHIPPING (string)
        # ============================================
        ('ship_to_name', pa.string()),
        ('ship_to_zip', pa.string()),
        ('ship_to_country', pa.string()),
        ('ship_via_code', pa.string()),

        # ============================================
        # VENDOR (string except IDs)
        # ============================================
        ('vendor_id', pa.string()),
        ('vendor_name', pa.string()),
        ('vendor_zip', pa.string()),
        ('purchase_point', pa.string()),
        ('terms_code', pa.string()),

        # ============================================
        # ITEM (string)
        # ============================================
        ('item_id', pa.string()),
        ('item_name', pa.string()),        # Unified best-available item name for visuals
        ('item_name_mpn', pa.string()),
        ('item_short_name', pa.string()),  # Shortened item_name (max 25 chars) for visuals
        ('item_display_name', pa.string()),
        ('item_description', pa.string()),
        ('line_description', pa.string()),
        ('vendor_part_num', pa.string()),
        ('item_gl_account', pa.string()),
        ('item_type', pa.string()),
        ('product_line_class', pa.string()),

        # ============================================
        # UNIT OF MEASURE (string)
        # ============================================
        ('uom_internal', pa.string()),
        ('uom_purchasing', pa.string()),

        # ============================================
        # OTHER BASE FIELDS (string)
        # ============================================
        ('job_number', pa.string()),
        ('memo', pa.string()),

        # ============================================
        # VENDOR ENRICHMENT (from vendor_map.csv)
        # ============================================
        ('standardized_vendor_name', pa.string()),
        ('intercompany', pa.string()),
        ('supplier_type', pa.string()),
        ('vendor_location', pa.string()),

        # ============================================
        # ITEM ENRICHMENT (from item_map.csv)
        # ============================================
        ('item_category_l1', pa.string()),
        ('item_category_l2', pa.string()),
        ('item_category_l3', pa.string()),
        ('item_category_l4', pa.string()),
        ('item_category_l5', pa.string()),
        ('item_raw_description', pa.string()),
        ('item_analysis_type', pa.string()),
        ('item_extraction_confidence', pa.string()),

        # Item consolidation (canonical grouping)
        ('item_canonical_name', pa.string()),
        ('item_canonical_group_id', pa.int64()),

        # Electrical specs (string - mixed values like "120/240V")
        ('item_spec_type', pa.string()),
        ('item_spec_amps', pa.string()),
        ('item_spec_voltage', pa.string()),
        ('item_spec_phase', pa.string()),
        ('item_spec_poles', pa.string()),
        ('item_spec_kaic', pa.string()),
        ('item_spec_frame', pa.string()),
        ('item_spec_kva', pa.string()),
        ('item_spec_gauge', pa.string()),
        ('item_spec_nema', pa.string()),

        # Raw material specs (float64 - NOT text)
        ('rawmat_finish', pa.string()),
        ('rawmat_thickness', pa.float64()),          # Was text, now numeric
        ('rawmat_width', pa.float64()),              # Was text, now numeric
        ('rawmat_length', pa.float64()),             # Was text, now numeric
        ('rawmat_uom', pa.string()),
        ('rawmat_weight_lbs', pa.float64()),         # Was text, now numeric
        ('rawmat_total_weight_lbs', pa.float64()),   # Was text, now numeric
        ('rawmat_price_per_lb', pa.float64()),       # Was text, now numeric
        ('copper_market_price_per_lb', pa.float64()),  # Comex copper index price

        # ============================================
        # CALCULATED METRICS
        # ============================================
        ('lead_time_days', pa.float64()),
        ('days_until_due', pa.float64()),
        ('days_since_order', pa.int64()),
        ('receipt_variance_qty', pa.float64()),
        ('receipt_variance_pct', pa.float64()),

        # 6-column quantity/amount structure
        ('open_amount', pa.float64()),              # open_quantity * corrected_unit_cost
        ('received_amount', pa.float64()),          # received_quantity * corrected_unit_cost
        ('open_plus_received_quantity', pa.float64()),  # open_quantity + received_quantity
        ('open_plus_received_amount', pa.float64()),    # open_amount + received_amount

        ('calculated_price', pa.float64()),         # Corrected unit_cost (after 1000x fix)
        ('spend_bucket', pa.string()),
        ('is_overdue', pa.bool_()),
        ('is_fully_received', pa.bool_()),
        ('on_time_flag', pa.string()),              # Alias for on_time_vs_promise_flag: "On Time"/"Late"/NULL
        ('on_time_vs_due_flag', pa.string()),       # Existing operational due-date classification
        ('on_time_vs_promise_flag', pa.string()),   # Promise-based OTD: "On Time"/"Late"/NULL
        ('days_early_late_vs_due', pa.float64()),   # Existing due-date metric
        ('days_early_late_vs_promise', pa.float64()),  # Existing physical receipt-vs-promise metric
        ('delta_of_promise_date_vs_receipt_date', pa.float64()),  # promise_date - final_receipt_date (days)
        ('otd_line_key', pa.string()),              # source_system + po_number + po_line + po_release_num
        ('final_receipt_date', pa.date32()),        # Latest receipt only when PO line is fully received
        ('otd_is_fully_received', pa.bool_()),      # PO-line-level completion status for OTD
        ('is_otd_representative', pa.bool_()),      # Exactly one physical row per PO line used for OTD
        ('is_late_or_overdue', pa.bool_()),         # TRUE if OTD status is Late or row is overdue

        # ============================================
        # NEW: CALENDAR ATTRIBUTES (precomputed)
        # ============================================
        ('order_year', pa.int32()),
        ('order_month', pa.int32()),
        ('order_year_month', pa.string()),  # "YYYY-MM" format
        ('order_quarter', pa.string()),     # "Q1", "Q2", "Q3", "Q4"

        # ============================================
        # NEW: BUSINESS UNIT (from mapping table)
        # ============================================
        ('business_unit', pa.string()),  # "States", "PwrQ", "Other"

        # ============================================
        # NEW: ANALYSIS DATE (precomputed with provenance)
        # ============================================
        ('analysis_date', pa.date32()),
        ('analysis_date_source', pa.string()),  # 'due_date', 'promise_date', 'receipt_date', 'order_date'
        ('analysis_date_is_fallback', pa.bool_()),  # True if fell back to order_date

        # ============================================
        # METADATA
        # ============================================
        ('_schema_version', pa.string()),
        ('_load_timestamp', pa.timestamp('us')),
        ('_data_as_of_date', pa.timestamp('us')),
    ])


def get_schema_metadata() -> dict:
    """
    Returns metadata to be embedded in the Parquet file.
    """
    return {
        'schema_version': SCHEMA_VERSION,
        'created_by': 'forgent-etl',
        'reporting_start_date': '2024-01-01',
        'reporting_rationale': 'Most conservative - highest completeness confidence for both systems',
        'grain': 'hybrid_transaction_line',
        'grain_open_order': 'source_system + po_number + po_line + po_release_num',
        'grain_receipt': 'source_system + receipt_number + receipt_line',
        'primary_key': 'source_system,transaction_type,po_number,po_line,po_release_num,receipt_number,receipt_line',
    }


# List of columns that were converted from text to numeric
CONVERTED_TO_NUMERIC = [
    'vendor_quantity',
    'uninvoiced_qty_supplier',
    'uninvoiced_qty_internal',
    'rawmat_thickness',
    'rawmat_width',
    'rawmat_length',
    'rawmat_weight_lbs',
    'rawmat_total_weight_lbs',
    'rawmat_price_per_lb',
    'copper_market_price_per_lb',
]

# List of new columns added by this ETL enhancement
NEW_COLUMNS = [
    'order_year',
    'order_month',
    'order_year_month',
    'order_quarter',
    'business_unit',
    'analysis_date',
    'analysis_date_source',
    'analysis_date_is_fallback',
    '_schema_version',
    '_load_timestamp',
    # 6-column quantity/amount structure (v2.1.0)
    'received_amount',
    'open_plus_received_quantity',
    'open_plus_received_amount',
    # OTD metrics (v2.2.0)
    'on_time_vs_due_flag',
    'on_time_vs_promise_flag',
    'days_early_late_vs_due',
    'days_early_late_vs_promise',
    # Short item name (v2.3.0)
    'item_short_name',
    # Unified item name (v2.7.0)
    'item_name',
    # Copper price index (v2.4.0)
    'copper_market_price_per_lb',
    # Unified late/overdue flag (v2.5.0)
    'is_late_or_overdue',
]
