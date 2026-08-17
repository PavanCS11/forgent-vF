import pandas as pd
import yaml
import os
import glob
import re
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from .config_loader import ConfigLoader


class Normalizer:
    """
    CSV-driven normalizer that reads field mappings from transaction_field_mapping.csv
    and outputs a single combined normalized file.
    """

    def __init__(self, config_path: str, schema_dir: str, input_dir: str, output_dir: str):
        self.config_loader = ConfigLoader(config_path)
        self.schema_dir = schema_dir
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.config_dir = os.path.dirname(config_path)

        # Load the CSV field mapping
        self.field_mapping = self._load_field_mapping()

        # Load schema for type information
        self.schema_types = self._load_schema_types()

        # Get target field names from the CSV mapping
        self.target_fields = self.field_mapping['Field Name'].tolist()

    def _load_field_mapping(self) -> pd.DataFrame:
        """Loads the transaction_field_mapping.csv as the authoritative mapping source."""
        mapping_file = self.config_loader.config.get('field_mapping_file', 'transaction_field_mapping.csv')
        mapping_path = os.path.join(self.config_dir, mapping_file)

        if not os.path.exists(mapping_path):
            raise FileNotFoundError(f"Field mapping file not found: {mapping_path}")

        df = pd.read_csv(mapping_path, encoding='utf-8-sig')  # Handle BOM
        print(f"Loaded field mapping with {len(df)} fields")
        return df

    def _load_schema_types(self) -> Dict[str, str]:
        """Loads type information from the transactions schema."""
        schema_path = os.path.join(self.schema_dir, 'transactions.yaml')
        if not os.path.exists(schema_path):
            print(f"Warning: Schema file not found at {schema_path}")
            return {}

        with open(schema_path, 'r') as f:
            schema_def = yaml.safe_load(f)
            return {field['name']: field.get('type', 'string')
                    for field in schema_def.get('fields', [])}

    def _extract_file_date(self, filename: str) -> Optional[datetime]:
        """Extracts date from filename pattern like '121025' (MMDDYY)."""
        # Look for 6-digit date pattern (MMDDYY)
        match = re.search(r'(\d{6})', filename)
        if match:
            date_str = match.group(1)
            try:
                # Parse MMDDYY format
                return datetime.strptime(date_str, '%m%d%y')
            except ValueError:
                pass
        return None

    def _print_data_freshness_report(self, combined_df: pd.DataFrame, file_dates: Dict[str, datetime]):
        """Prints data freshness and date range validation report."""
        print("\n" + "="*60)
        print("DATA FRESHNESS & RANGE VALIDATION")
        print("="*60)

        today = datetime.now()

        # Report file dates
        if file_dates:
            print("\nSource File Dates:")
            for filename, file_date in sorted(file_dates.items()):
                days_old = (today - file_date).days
                status = "STALE" if days_old > 7 else "OK"
                print(f"  {filename}: {file_date.strftime('%Y-%m-%d')} ({days_old} days old) [{status}]")

        # Analyze order_date by source system
        if 'order_date' in combined_df.columns and 'source_system' in combined_df.columns:
            print("\nOrder Date Ranges by Source:")
            combined_df['order_date_parsed'] = pd.to_datetime(combined_df['order_date'], errors='coerce')

            for source in combined_df['source_system'].dropna().unique():
                source_data = combined_df[combined_df['source_system'] == source]
                dates = source_data['order_date_parsed'].dropna()

                if not dates.empty:
                    min_date = dates.min()
                    max_date = dates.max()
                    days_since_newest = (today - max_date).days if pd.notna(max_date) else None

                    print(f"\n  {source}:")
                    print(f"    Earliest order: {min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else 'N/A'}")
                    print(f"    Latest order:   {max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else 'N/A'}")
                    print(f"    Row count:      {len(source_data):,}")

                    if days_since_newest and days_since_newest > 7:
                        print(f"    WARNING: Most recent order is {days_since_newest} days old!")

            # Clean up temp column
            combined_df.drop(columns=['order_date_parsed'], inplace=True)

        # Check for null dates
        if 'order_date' in combined_df.columns:
            null_count = combined_df['order_date'].isna().sum()
            null_pct = (null_count / len(combined_df)) * 100
            if null_pct > 5:
                print(f"\nWARNING: {null_pct:.1f}% of records have missing order_date ({null_count:,} rows)")

        print("\n" + "="*60)

    def _write_pipeline_metadata(self, file_dates: Dict[str, datetime]):
        """Writes pipeline metadata including max file date for downstream steps."""
        if not file_dates:
            print("  Warning: No file dates available for metadata")
            return

        max_file_date = max(file_dates.values())

        metadata = {
            "max_file_date": max_file_date.strftime('%Y-%m-%d'),
            "file_dates": {k: v.strftime('%Y-%m-%d') for k, v in file_dates.items()},
            "generated_at": datetime.now().isoformat()
        }

        metadata_path = os.path.join(self.output_dir, "pipeline_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"  Pipeline metadata saved: max_file_date = {max_file_date.strftime('%Y-%m-%d')}")

    def _load_po_line_dates_lookup(self) -> dict:
        """Loads NS PO Line Dates file and builds a lookup for joining to NS Receipt rows.

        Key: po_number -> dict of date fields (min promise date across all lines for that PO).
        Uses minimum dates so that the earliest commitment is used when a PO has multiple lines.
        Returns dates as '%Y-%m-%d' strings to match the rest of the normalized output.
        """
        pattern = os.path.join(self.input_dir, 'Netsuite/Netsuite PO Line Dates*.csv')
        files = glob.glob(pattern)

        if not files:
            print("  Warning: No NS PO Line Dates file found — NS receipt date fields will be NULL")
            return {}

        files.sort(key=os.path.getmtime, reverse=True)
        file_path = files[0]
        print(f"\n  Loading PO Line Dates lookup from: {os.path.basename(file_path)}")

        df = pd.read_csv(file_path, encoding='utf-8-sig')

        po_num_col = 'PO Number'
        promise_date_col = 'Maximum of PO New Promise Date'
        old_promise_col = 'Maximum of PO Promise Date'
        due_date_col = 'Maximum of PO Due Date'

        if po_num_col not in df.columns:
            print(f"  Warning: PO Line Dates missing 'PO Number' column — skipping join")
            return {}

        for col in [promise_date_col, old_promise_col, due_date_col]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        df['_key_po'] = df[po_num_col].astype(str).str.strip()

        # Aggregate to PO level: take the minimum date across all lines for each PO
        agg_dict = {}
        if promise_date_col in df.columns:
            agg_dict[promise_date_col] = 'min'
        if old_promise_col in df.columns:
            agg_dict[old_promise_col] = 'min'
        if due_date_col in df.columns:
            agg_dict[due_date_col] = 'min'

        df_agg = df.groupby('_key_po', as_index=False).agg(agg_dict)

        # Format dates back to strings
        for col in [promise_date_col, old_promise_col, due_date_col]:
            if col in df_agg.columns:
                df_agg[col] = df_agg[col].dt.strftime('%Y-%m-%d')

        lookup = {}
        for _, row in df_agg.iterrows():
            key = row['_key_po']
            lookup[key] = {
                'promise_date': row.get(promise_date_col) if promise_date_col in df_agg.columns else None,
                'old_promise_date': row.get(old_promise_col) if old_promise_col in df_agg.columns else None,
                'due_date': row.get(due_date_col) if due_date_col in df_agg.columns else None,
            }

        print(f"  PO Line Dates lookup: {len(lookup):,} unique PO numbers")
        return lookup

    def _join_po_line_dates(self, df: pd.DataFrame, lookup: dict) -> pd.DataFrame:
        """Joins PO Line Dates onto NS RECEIPT rows.

        Populates promise_date, old_promise_date, and due_date for NETSUITE RECEIPT rows
        using PO-level data from the PO Line Dates saved search.
        Join key: po_number only (item name/MPN fields differ between the two saved searches).
        """
        if not lookup:
            return df

        ns_receipt_mask = (
            (df['source_system'] == 'NETSUITE') &
            (df['transaction_type'] == 'RECEIPT')
        )
        total = ns_receipt_mask.sum()
        if total == 0:
            return df

        print(f"\n  Joining PO Line Dates onto {total:,} NS Receipt rows...")

        for col in ['promise_date', 'old_promise_date', 'due_date']:
            if col not in df.columns:
                df[col] = None

        lookup_df = pd.DataFrame([
            {'_key_po': k,
             '_j_promise': v.get('promise_date'),
             '_j_old_promise': v.get('old_promise_date'),
             '_j_due': v.get('due_date')}
            for k, v in lookup.items()
        ])

        ns_receipts = df[ns_receipt_mask].copy()
        ns_receipts['_key_po'] = ns_receipts['po_number'].astype(str).str.strip()

        merged = ns_receipts.merge(lookup_df, on='_key_po', how='left')

        df.loc[ns_receipt_mask, 'promise_date'] = merged['_j_promise'].values
        df.loc[ns_receipt_mask, 'old_promise_date'] = merged['_j_old_promise'].values
        df.loc[ns_receipt_mask, 'due_date'] = merged['_j_due'].values

        promise_populated = merged['_j_promise'].notna().sum()
        print(f"    promise_date populated: {promise_populated:,} / {total:,} ({promise_populated/total*100:.1f}%)")

        return df

    def _apply_ns_receipt_promise_fallback(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fallback: for NS receipt rows with null promise_date after the PO Line Dates join,
        pull promise_date from the NS Open POs file keyed on po_number only.

        This recovers rows (e.g. PwrQ) whose item_name_mpn didn't match the line-level
        join key. Uses the minimum Expected Receipt Date per PO as a conservative estimate.
        """
        ns_receipt_null_mask = (
            (df['source_system'] == 'NETSUITE') &
            (df['transaction_type'] == 'RECEIPT') &
            (df['promise_date'].isna())
        )
        unmatched = ns_receipt_null_mask.sum()
        if unmatched == 0:
            return df

        print(f"\n  NS Receipt promise_date fallback: {unmatched:,} unmatched rows — loading NS Open POs...")

        pattern = os.path.join(self.input_dir, 'Netsuite/Netsuite Open POs*.csv')
        files = glob.glob(pattern)
        if not files:
            print("  Warning: No NS Open POs file found — fallback skipped")
            return df

        files.sort(key=os.path.getmtime, reverse=True)
        ns_open = pd.read_csv(files[0], dtype=str)

        po_col = 'PO Number'
        erd_col = 'Maximum of Expected Receipt Date'
        if po_col not in ns_open.columns or erd_col not in ns_open.columns:
            print("  Warning: NS Open POs missing required columns — fallback skipped")
            return df

        ns_open['_key_po'] = ns_open[po_col].astype(str).str.strip()
        ns_open[erd_col] = pd.to_datetime(ns_open[erd_col], errors='coerce').dt.strftime('%Y-%m-%d')

        # Take the minimum Expected Receipt Date per PO (conservative: earliest commitment)
        po_promise = (
            ns_open.dropna(subset=[erd_col])
            .groupby('_key_po')[erd_col]
            .min()
            .reset_index()
            .rename(columns={erd_col: '_fallback_promise'})
        )

        unmatched_receipts = df[ns_receipt_null_mask].copy()
        unmatched_receipts['_key_po'] = unmatched_receipts['po_number'].astype(str).str.strip()
        merged = unmatched_receipts.merge(po_promise, on='_key_po', how='left')

        df.loc[ns_receipt_null_mask, 'promise_date'] = merged['_fallback_promise'].values

        filled = merged['_fallback_promise'].notna().sum()
        print(f"    Fallback filled promise_date: {filled:,} / {unmatched:,} ({filled/unmatched*100:.1f}%)")

        return df

    def process_all(self):
        """Processes all configured sources and outputs a single combined file."""
        sources = self.config_loader.get_sources()
        all_normalized_data = []
        all_file_dates = {}  # Track file dates for freshness report

        for source in sources:
            df_normalized, file_dates = self.process_source(source)
            if df_normalized is not None and not df_normalized.empty:
                all_normalized_data.append(df_normalized)
                all_file_dates.update(file_dates)

        if all_normalized_data:
            # Combine all normalized data into single DataFrame
            combined_df = pd.concat(all_normalized_data, ignore_index=True)

            # Join PO Line Dates onto NS Receipt rows (line-level promise/due dates)
            po_line_lookup = self._load_po_line_dates_lookup()
            combined_df = self._join_po_line_dates(combined_df, po_line_lookup)

            # Fallback: fill remaining null promise_date on NS receipts from NS Open POs (PO-level)
            combined_df = self._apply_ns_receipt_promise_fallback(combined_df)

            # Print data freshness report
            self._print_data_freshness_report(combined_df, all_file_dates)

            # Write pipeline metadata for downstream steps
            self._write_pipeline_metadata(all_file_dates)

            # Output single combined file
            output_path = os.path.join(self.output_dir, "all_transactions_normalized.csv")
            combined_df.to_csv(output_path, index=False)
            print(f"\nSaved combined normalized data to: {output_path}")
            print(f"Total rows: {len(combined_df):,}")
            print(f"Total columns: {len(combined_df.columns)}")
        else:
            print("No data was processed from any source.")

    def process_source(self, source_config: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Dict[str, datetime]]:
        """Processes a single source configuration using CSV-driven mapping.

        Returns:
            Tuple of (normalized DataFrame, dict of filename -> file date)
        """
        source_name = source_config['name']
        csv_mapping_column = source_config.get('csv_mapping_column')
        file_dates = {}

        if not csv_mapping_column:
            print(f"Skipping {source_name}: no csv_mapping_column specified")
            return None, file_dates

        if csv_mapping_column not in self.field_mapping.columns:
            print(f"Skipping {source_name}: column '{csv_mapping_column}' not found in mapping CSV")
            return None, file_dates

        print(f"\nProcessing source: {source_name}")
        print(f"  Using mapping column: {csv_mapping_column}")

        # Find matching files
        file_pattern = os.path.join(self.input_dir, source_config['file_pattern'])
        files = glob.glob(file_pattern)

        if not files:
            print(f"  No files found for pattern: {file_pattern}")
            return None, file_dates

        all_data = []
        for file_path in files:
            filename = os.path.basename(file_path)
            print(f"  Reading file: {filename}")

            # Extract file date for freshness tracking
            file_date = self._extract_file_date(filename)
            if file_date:
                file_dates[filename] = file_date

            try:
                # Handle both CSV and Excel files
                if file_path.lower().endswith('.xlsx') or file_path.lower().endswith('.xls'):
                    df_raw = pd.read_excel(file_path)
                else:
                    df_raw = pd.read_csv(file_path, encoding='utf-8-sig')
                print(f"    Raw rows: {len(df_raw):,}, columns: {len(df_raw.columns)}")

                # Apply CSV-driven field mapping
                df_normalized = self._apply_csv_mapping(
                    df_raw,
                    csv_mapping_column,
                    source_config
                )

                all_data.append(df_normalized)
                print(f"    Normalized rows: {len(df_normalized):,}")

            except Exception as e:
                print(f"  Error processing file {file_path}: {e}")
                import traceback
                traceback.print_exc()

        if all_data:
            return pd.concat(all_data, ignore_index=True), file_dates
        return None, file_dates

    def _apply_csv_mapping(self, df_raw: pd.DataFrame, mapping_column: str,
                           source_config: Dict[str, Any]) -> pd.DataFrame:
        """
        Applies field mapping from CSV to raw DataFrame.

        Handles:
        - NULL → Set to None
        - Hardcode 'VALUE' → Set literal value
        - Derive from X → Set to None (handled in pre_cleaner)
        - Direct column name → Map source column to target
        """
        # Create empty DataFrame with target fields
        df_result = pd.DataFrame(index=df_raw.index)

        for _, row in self.field_mapping.iterrows():
            target_field = row['Field Name']
            source_spec = row[mapping_column]

            # Handle NULL or empty
            if pd.isna(source_spec) or str(source_spec).strip().upper() == 'NULL':
                df_result[target_field] = None
                continue

            source_spec = str(source_spec).strip()

            # Handle Hardcode 'VALUE'
            hardcode_match = re.match(r"Hardcode\s+'([^']*)'", source_spec, re.IGNORECASE)
            if hardcode_match:
                value = hardcode_match.group(1)
                df_result[target_field] = value
                continue

            # Handle Derive from X (leave for pre_cleaner to handle)
            if source_spec.lower().startswith('derive from'):
                df_result[target_field] = None  # Will be derived in enrichment step
                continue

            # Direct column mapping
            if source_spec in df_raw.columns:
                df_result[target_field] = df_raw[source_spec]
            else:
                # Column not found in source - set to None
                df_result[target_field] = None

        # Apply date formatting
        for field, dtype in self.schema_types.items():
            if dtype == 'date' and field in df_result.columns:
                df_result[field] = pd.to_datetime(df_result[field], errors='coerce')
                df_result[field] = df_result[field].dt.strftime('%Y-%m-%d')

        # Fix Excel-corrupted scientific notation IDs (e.g., '1.75223E+11' -> '175223000000')
        if 'item_id' in df_result.columns:
            sci_pattern = re.compile(r'^\d+\.\d+[eE]\+\d+$')
            sci_mask = df_result['item_id'].astype(str).str.match(sci_pattern, na=False)
            fix_count = sci_mask.sum()
            if fix_count > 0:
                df_result.loc[sci_mask, 'item_id'] = (
                    df_result.loc[sci_mask, 'item_id']
                    .astype(str)
                    .apply(lambda x: str(int(float(x))))
                )
                print(f"    Fixed {fix_count:,} scientific notation item_id values")

        return df_result


if __name__ == "__main__":
    # Test run
    norm = Normalizer(
        config_path="step_01_ingestion/b.config/source_mappings.yaml",
        schema_dir="step_01_ingestion/c.schemas",
        input_dir="step_01_ingestion/a.raw_data",
        output_dir="step_01_ingestion/e.normalized"
    )
    norm.process_all()
