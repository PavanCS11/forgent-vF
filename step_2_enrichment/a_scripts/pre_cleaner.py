import pandas as pd
import os
from typing import Optional


class PreCleaner:
    """
    Enrichment step that applies derivation logic and calculated fields
    to the normalized transaction data.

    Since vendor/item data is now pre-integrated into the transaction files,
    this step focuses on:
    1. Applying "Derive from X" field logic
    2. Adding calculated fields
    3. Data cleanup and standardization
    """

    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def _load_normalized_data(self) -> pd.DataFrame:
        """Loads the combined normalized transaction file."""
        file_path = os.path.join(self.input_dir, "all_transactions_normalized.csv")

        if not os.path.exists(file_path):
            print(f"  Error: Normalized file not found: {file_path}")
            return pd.DataFrame()

        print(f"  Loading: {file_path}")
        df = pd.read_csv(file_path, low_memory=False)
        print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
        return df

    def _apply_derivations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies derivation logic for fields marked as "Derive from X" in the mapping.

        Derivation rules:
        - is_open_order: Quantity-based - True if open_quantity > 0 or remaining qty > 0
        """
        print("  Applying derivation logic...")

        # --- Derive is_open_order (quantity-based) ---
        # An order is open if it has remaining quantity to receive
        # This is the single source of truth, regardless of status fields

        # Initialize is_open_order column to False
        df['is_open_order'] = False

        # Primary: Check open_quantity > 0
        if 'open_quantity' in df.columns:
            open_qty = pd.to_numeric(df['open_quantity'], errors='coerce').fillna(0)
            df.loc[open_qty > 0, 'is_open_order'] = True
            print(f"    is_open_order=True from open_quantity > 0: {(open_qty > 0).sum():,} rows")

        # Fallback: If open_quantity not available, derive from total - received
        if 'total_quantity' in df.columns and 'received_quantity' in df.columns:
            remaining = (
                pd.to_numeric(df['total_quantity'], errors='coerce').fillna(0) -
                pd.to_numeric(df['received_quantity'], errors='coerce').fillna(0)
            )
            additional_open = (remaining > 0) & (~df['is_open_order'])
            df.loc[remaining > 0, 'is_open_order'] = True
            print(f"    is_open_order=True from remaining qty > 0: {additional_open.sum():,} additional rows")

        print(f"    Total is_open_order=True: {df['is_open_order'].sum():,} rows")

        # --- Derive buyer_id (fallback to created_by) ---
        if 'buyer_id' in df.columns and 'created_by' in df.columns:
            missing_buyer = df['buyer_id'].isna() | (df['buyer_id'].astype(str).str.strip() == '')
            has_creator = df['created_by'].notna() & (df['created_by'].astype(str).str.strip() != '')
            fill_mask = missing_buyer & has_creator
            df.loc[fill_mask, 'buyer_id'] = df.loc[fill_mask, 'created_by']
            print(f"    buyer_id filled from created_by: {fill_mask.sum():,} rows")

        # Convert boolean fields to proper boolean type
        boolean_fields = ['is_open_order', 'is_open_release', 'is_confirmed']
        for field in boolean_fields:
            if field in df.columns:
                df[field] = self._to_boolean(df[field])

        return df

    def _to_boolean(self, series: pd.Series) -> pd.Series:
        """
        Converts a series to boolean using fast vectorized operations.
        True values: True, 'True', 'true', 'Yes', 'yes', 'Y', 'y', '1', 1
        False values: False, 'False', 'false', 'No', 'no', 'N', 'n', '0', 0
        """
        if series.empty:
            return series

        # Standardize strings to lower/strip
        s = series.astype(str).str.lower().str.strip()

        # Define boolean mapping
        true_values = {'true', 'yes', 'y', '1', '1.0', 'open'}
        false_values = {'false', 'no', 'n', '0', '0.0', 'closed'}

        # Create result series
        res = pd.Series(index=series.index, dtype='object')
        res.loc[s.isin(true_values)] = True
        res.loc[s.isin(false_values)] = False

        # Restore original NaNs
        res.loc[series.isna()] = None

        return res

    def _add_calculated_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds calculated fields useful for downstream analysis."""
        print("  Adding calculated fields...")

        # ALWAYS recalculate open_quantity for OPEN_ORDER transactions
        # This ensures tie-out: open_quantity = total_quantity - received_quantity
        # Source ERP values may be incorrect due to rounding, timing, or data issues
        if 'total_quantity' in df.columns and 'received_quantity' in df.columns:
            # Only recalculate for Open Orders transactions (receipts don't have open_quantity)
            is_open_order_txn = df['transaction_type'] == 'Open Orders'

            total_qty = pd.to_numeric(df['total_quantity'], errors='coerce').fillna(0)
            received_qty = pd.to_numeric(df['received_quantity'], errors='coerce').fillna(0)
            calculated_open = total_qty - received_qty

            # Preserve original open_quantity for audit trail BEFORE overwriting
            if 'open_quantity' in df.columns:
                df['_original_open_quantity'] = df['open_quantity'].copy()
                df['_open_quantity_corrected'] = False

                existing = is_open_order_txn & df['open_quantity'].notna()
                original_open = pd.to_numeric(df['open_quantity'], errors='coerce')
                # Use small tolerance for floating point comparison
                mismatches = existing & (abs(original_open - calculated_open) > 0.01)

                if mismatches.any():
                    mismatch_count = mismatches.sum()
                    # Calculate dollar impact of corrections
                    unit_cost = pd.to_numeric(df.get('unit_cost', 0), errors='coerce').fillna(0)
                    dollar_diff = abs((original_open - calculated_open) * unit_cost)
                    total_dollar_impact = dollar_diff[mismatches].sum()

                    print(f"    Correcting {mismatch_count:,} open_quantity values that didn't tie out")
                    print(f"    Estimated dollar impact of corrections: ${total_dollar_impact:,.2f}")

                    # Mark corrected rows
                    df.loc[mismatches, '_open_quantity_corrected'] = True

                    # Log sample corrections
                    sample = df.loc[mismatches, ['po_number', 'po_line', 'open_quantity', 'total_quantity', 'received_quantity']].head(5)
                    print(f"    Sample corrections:")
                    for _, row in sample.iterrows():
                        orig = row['open_quantity']
                        calc = row['total_quantity'] - row['received_quantity']
                        print(f"      PO {row['po_number']}/{row['po_line']}: {orig} -> {calc}")
            else:
                df['_original_open_quantity'] = None
                df['_open_quantity_corrected'] = False

            # Apply calculated value to all OPEN_ORDER rows
            df.loc[is_open_order_txn, 'open_quantity'] = calculated_open[is_open_order_txn]

        return df

    def _normalize_whitespace(self, series: pd.Series) -> pd.Series:
        """
        Normalizes whitespace in string series:
        - Replaces non-breaking spaces (char 160 / \\xa0) with regular spaces
        - Strips leading/trailing whitespace
        - Converts 'nan' strings to None
        """
        if series.dtype != 'object':
            return series

        return (
            series
            .astype(str)
            .str.replace('\xa0', ' ', regex=False)   # NBSP -> space
            .str.replace('\u00a0', ' ', regex=False)  # Unicode NBSP
            .str.strip()
            .replace({'nan': None, 'None': None, '': None})
        )

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Performs data cleanup and standardization."""
        print("  Cleaning data...")

        # Standardize source_system to uppercase (with NBSP handling)
        if 'source_system' in df.columns:
            df['source_system'] = self._normalize_whitespace(df['source_system'])
            df['source_system'] = df['source_system'].str.upper()

        # Standardize transaction_type to proper casing (with NBSP handling)
        if 'transaction_type' in df.columns:
            df['transaction_type'] = self._normalize_whitespace(df['transaction_type'])
            # Convert to proper casing: "OPEN_ORDER" -> "Open Orders", "RECEIPT" -> "Receipts"
            transaction_type_map = {
                'OPEN_ORDER': 'Open Orders',
                'RECEIPT': 'Receipts',
                'open_order': 'Open Orders',
                'receipt': 'Receipts'
            }
            df['transaction_type'] = df['transaction_type'].str.upper()  # Normalize first
            df['transaction_type'] = df['transaction_type'].map(transaction_type_map).fillna(df['transaction_type'])

        # Normalize whitespace in all string columns (handles NBSP + regular trim)
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            try:
                df[col] = self._normalize_whitespace(df[col])
            except Exception:
                # Skip columns that can't be processed
                pass

        # Replace 'nan' strings with actual NaN
        df = df.replace({'nan': None, 'None': None, '': None})

        return df

    def run(self):
        """Main execution method."""
        print("\nStarting Pre-Cleaner (Enrichment Step)...")

        # Load normalized data
        df = self._load_normalized_data()

        if df.empty:
            print("  No data found. Aborting.")
            return

        # Add calculated fields FIRST (recalculates open_quantity for tie-out)
        df = self._add_calculated_fields(df)

        # Apply derivation logic (uses corrected open_quantity for is_open_order)
        df = self._apply_derivations(df)

        # Clean data
        df = self._clean_data(df)

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save output
        output_path = os.path.join(self.output_dir, "pre_cleaned_staging.csv")
        df.to_csv(output_path, index=False)
        print(f"\n  Saved pre-cleaned staging data to: {output_path}")
        print(f"  Total rows: {len(df):,}")
        print(f"  Total columns: {len(df.columns)}")

        # Print summary by source and type
        if 'source_system' in df.columns and 'transaction_type' in df.columns:
            print("\n  Summary by Source and Type:")
            summary = df.groupby(['source_system', 'transaction_type']).size()
            for (source, ttype), count in summary.items():
                print(f"    {source} - {ttype}: {count:,}")


if __name__ == "__main__":
    cleaner = PreCleaner(
        input_dir="step_01_ingestion/e.normalized",
        output_dir="step_02_enrichment/b.staging"
    )
    cleaner.run()
