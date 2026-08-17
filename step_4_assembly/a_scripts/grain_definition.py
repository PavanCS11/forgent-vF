"""
PO Fact Table Grain Definition
==============================

This module documents the grain (level of detail) for the po_fact_table
and provides utility functions for grain validation.

Grain Type: HYBRID
==================

The po_fact_table uses a hybrid grain that varies by transaction type:

1. For "Open Orders" transactions:
   - Grain: PO Line-Release level
   - One row per: source_system + po_number + po_line + po_release_num
   - Note: NetSuite has no release concept, so po_release_num is always null

2. For "Receipts" transactions:
   - Grain: Receipt line level
   - One row per: source_system + receipt_number + receipt_line
   - Each row represents one receipt line

Composite Primary Key
=====================

The full primary key that uniquely identifies each row:

    source_system + transaction_type + po_number + po_line +
    COALESCE(po_release_num, '') + COALESCE(receipt_number, '') +
    COALESCE(receipt_line, '')

This ensures:
- "Open Orders" rows are unique by PO line-release
- "Receipts" rows are unique by receipt line
- No collision between transaction types

Important Notes
===============

1. For analytics on Open Orders (current commitments):
   - Filter: transaction_type = 'Open Orders'
   - Sum open_quantity, open_amount without double-counting

2. For analytics on Receipts (historical spend):
   - Filter: transaction_type = 'Receipts'
   - Sum received_quantity, calculated_line_amount

3. When joining with dates:
   - Use analysis_date for time intelligence (precomputed)
   - Open orders: analysis_date = due_date (or fallback)
   - Receipts: analysis_date = receipt_date (or fallback)
"""

# Primary key columns for uniqueness validation
PRIMARY_KEY_COLUMNS = [
    'source_system',
    'transaction_type',
    'po_number',
    'po_line',
    'po_release_num',
    'receipt_number',
    'receipt_line',
]

# Grain by transaction type
GRAIN_OPEN_ORDER = ['source_system', 'po_number', 'po_line', 'po_release_num']
GRAIN_RECEIPT = ['source_system', 'receipt_number', 'receipt_line']


def get_composite_key(row) -> str:
    """
    Generates a composite key string for a row.
    Used for uniqueness checking and debugging.
    """
    def safe_str(val):
        if val is None or (isinstance(val, float) and val != val):  # NaN check
            return ''
        return str(val)

    return '|'.join([
        safe_str(row.get('source_system')),
        safe_str(row.get('transaction_type')),
        safe_str(row.get('po_number')),
        safe_str(row.get('po_line')),
        safe_str(row.get('po_release_num')),
        safe_str(row.get('receipt_number')),
        safe_str(row.get('receipt_line')),
    ])


def validate_grain_uniqueness(df) -> tuple:
    """
    Validates that the primary key columns form unique rows.

    Returns:
        tuple: (is_valid: bool, duplicate_count: int, sample_duplicates: DataFrame)
    """
    import pandas as pd

    # Get columns that exist in the dataframe
    pk_cols = [c for c in PRIMARY_KEY_COLUMNS if c in df.columns]

    if not pk_cols:
        return False, 0, None

    # Fill nulls with transaction-type-aware placeholders so that:
    # - Open Order NULLs (receipt_number, receipt_line) don't collide with each other
    # - Receipt NULLs (po_release_num) don't collide with each other
    # Previously used fillna('') which made rows with different NULLs look identical
    df_check = df[pk_cols].astype(object).copy()
    txn_type = df['transaction_type'] if 'transaction_type' in df.columns else pd.Series([''] * len(df))
    for col in pk_cols:
        null_mask = df_check[col].isna()
        if null_mask.any():
            df_check.loc[null_mask, col] = '__NULL_' + txn_type[null_mask].astype(str) + '_' + col

    # Find duplicates
    duplicates_mask = df_check.duplicated(keep=False)
    duplicate_count = duplicates_mask.sum() // 2  # Each duplicate counted twice

    if duplicate_count > 0:
        # Get sample of duplicates for debugging
        sample_duplicates = df[duplicates_mask].head(10)
        return False, duplicate_count, sample_duplicates

    return True, 0, None
