import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import re


# Handle both relative and absolute imports
try:
    from .schema_contract import get_po_fact_schema, get_schema_metadata, SCHEMA_VERSION, CONVERTED_TO_NUMERIC
    from .grain_definition import validate_grain_uniqueness
    from .data_quality import DataQualityChecker, DataQualityError
except ImportError:
    from schema_contract import get_po_fact_schema, get_schema_metadata, SCHEMA_VERSION, CONVERTED_TO_NUMERIC
    from grain_definition import validate_grain_uniqueness
    from data_quality import DataQualityChecker, DataQualityError

# Central pipeline config — thresholds for merge validation
try:
    from step_3_maintenance.c_ai_enrichment.taxonomy import (
        REPORTING_START_DATE as _CFG_REPORTING_START_DATE,
        MIN_VENDOR_MATCH_RATE as _CFG_MIN_VENDOR_MATCH_RATE,
        MIN_ITEM_MATCH_RATE as _CFG_MIN_ITEM_MATCH_RATE,
    )
except ImportError:
    _CFG_REPORTING_START_DATE = '2024-01-01'
    _CFG_MIN_VENDOR_MATCH_RATE = 0.70
    _CFG_MIN_ITEM_MATCH_RATE = 0.30


def create_short_name(name, max_len: int = 25) -> str:
    """
    Create shortened item name with smart word-boundary truncation.

    Truncates at the last space, dash, or underscore before max_len,
    then appends ellipsis. If no delimiter found, hard truncates.
    """
    if pd.isna(name):
        return name

    name = str(name).strip()
    if len(name) <= max_len:
        return name

    # Find last delimiter before the limit (leaving room for ellipsis)
    truncate_at = max_len - 1
    delimiters = [' ', '-', '_', '/']

    # Search backwards for a delimiter
    best_break = -1
    for i in range(truncate_at - 1, max(0, truncate_at - 10), -1):
        if name[i] in delimiters:
            best_break = i
            break

    if best_break > max_len // 2:  # Only use delimiter if reasonable
        return name[:best_break].rstrip('-_ ') + '…'
    else:
        return name[:truncate_at] + '…'


class FinalBuilder:
    """
    Final assembly step that applies vendor/item standardization mappings
    and adds calculated metrics for dashboard analytics.

    Uses new snake_case column naming convention from transaction_field_mapping.csv
    """

    # Spend bucket thresholds
    SPEND_BUCKETS = [
        (0, 100, 'Micro (<$100)'),
        (100, 1000, 'Small ($100-$1K)'),
        (1000, 10000, 'Medium ($1K-$10K)'),
        (10000, 100000, 'Large ($10K-$100K)'),
        (100000, float('inf'), 'Strategic (>$100K)')
    ]

    # OTD classification threshold
    GRACE_PERIOD_DAYS = 3    # Delivered on or before promise + 3 days is On Time
    """
    OTD Classification (Promise Date):
    - On Time: final completion receipt <= promise_date + GRACE_PERIOD_DAYS
    - Late: final completion receipt > promise_date + GRACE_PERIOD_DAYS
    - Open and not yet past promise + grace: NULL (not OTD eligible yet)
    - Open and past promise + grace: Late
    - OTD is evaluated once per PO line, not once per receipt transaction.
    """

    # Reporting date range configuration (from central config in taxonomy.py)
    REPORTING_START_DATE = _CFG_REPORTING_START_DATE
    """
    Reporting Start Date Rationale:
    - NetSuite data begins: 2018-05-21
    - Epicor open orders begin: 2024-08-28
    - 2024-01-01 provides 2+ years with both systems fully operational
    - Ensures fair comparison - no partial coverage years
    - Aligns with Epicor's practical availability window
    - Uses inclusive OR filter: (order_date >= cutoff) OR (receipt_date >= cutoff)
    - Supports dual perspectives: order date view AND receipt date view
    """

    def __init__(self, staging_path: str, vendor_map_path: str, item_map_path: str, output_dir: str, metadata_path: str = None):
        self.staging_path = staging_path
        self.vendor_map_path = vendor_map_path
        self.item_map_path = item_map_path
        self.output_dir = output_dir
        self.metadata_path = metadata_path

        # Load pipeline metadata (for max_file_date)
        self.pipeline_metadata = self._load_pipeline_metadata()

        # Business unit mapping file (in same directory as vendor_map)
        self.business_unit_map_path = os.path.join(
            os.path.dirname(vendor_map_path),
            "business_unit_map.csv"
        )

        # Terms mapping file (in same directory as vendor_map)
        self.terms_map_path = os.path.join(
            os.path.dirname(vendor_map_path),
            "terms_map.csv"
        )

    def _load_pipeline_metadata(self) -> dict:
        """Loads pipeline metadata from normalization step."""
        if not self.metadata_path or not os.path.exists(self.metadata_path):
            print("  Warning: Pipeline metadata not found, using system date as fallback")
            return {"max_file_date": pd.Timestamp.now().strftime('%Y-%m-%d')}

        with open(self.metadata_path, 'r') as f:
            metadata = json.load(f)
        print(f"  Loaded pipeline metadata: max_file_date = {metadata.get('max_file_date')}")
        return metadata

    def _validate_merge(self, df: pd.DataFrame, merge_name: str,
                        result_col: str, min_match_rate: float = 0.50) -> None:
        """Validate that a merge matched a reasonable number of rows.
        After the merge, how many rows have a value in standardized_vendor_name?"
        Args:
            df: DataFrame after merge.
            merge_name: Human-readable name for logging.
            result_col: Column that should be populated after merge.
            min_match_rate: Minimum acceptable match rate (0.0-1.0).

        Raises:
            DataQualityError: If match rate is below threshold.
        """
        total = len(df)
        if total == 0:
            return
        matched = df[result_col].notna().sum()
        rate = matched / total
        print(f"    Merge '{merge_name}': {matched:,}/{total:,} matched ({rate:.1%})")
        if rate < min_match_rate:
            raise DataQualityError(
                f"Merge '{merge_name}' match rate {rate:.1%} is below "
                f"threshold {min_match_rate:.0%}. Check if upstream data and "
                f"mapping table are aligned."
            )

    # Columns to pull from vendor_map.csv (besides Raw_Name used for joining)
    VENDOR_MAP_COLUMNS = [
        'Standardized_Name',
        'Intercompany',
        'Supplier_Type',
        'Location',
    ]

    # Columns to pull from item_map.csv (besides Item_ID used for joining)
    ITEM_MAP_COLUMNS = [
        'Category_Level_1',
        'Category_Level_2',
        'Category_Level_3',
        'Category_Level_4',
        'Category_Level_5',
        'Raw_Description',
        'analysis_type',
        'extraction_confidence',
        # Electrical specs
        'spec_type',
        'spec_amps',
        'spec_voltage',
        'spec_phase',
        'spec_poles',
        'spec_kaic',
        'spec_frame',
        'spec_kva',
        'spec_gauge',
        'spec_nema',
        # Raw material specs
        'RawMat_Finish',
        'RawMat_Thickness',
        'RawMat_Width',
        'RawMat_Length',
        'RawMat_UoM',
        'RawMat_Weight_Lbs',
        # Item consolidation
        'Canonical_Name',
        'Canonical_Group_ID',
    ]

    def _apply_vendor_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies vendor name standardization and enrichment from mapping table."""
        def _clean_text_col(col_name: str) -> pd.Series:
            """Normalize text fields so blank-like values behave as nulls for coalescing."""
            series = df.get(col_name, pd.Series(index=df.index, dtype='object'))
            return (
                series
                .astype(str)
                .str.strip()
                .replace({'': np.nan, 'nan': np.nan, 'None': np.nan})
            )

        if not os.path.exists(self.vendor_map_path):
            df['standardized_vendor_name'] = _clean_text_col('vendor_name').fillna('N/A')
            return df

        # Keep literal placeholders like 'N/A' as strings instead of auto-converting to NaN.
        df_vendor_map = pd.read_csv(self.vendor_map_path, keep_default_na=False)
        if df_vendor_map.empty:
            df['standardized_vendor_name'] = _clean_text_col('vendor_name').fillna('N/A')
            return df

        # Clean column names
        df_vendor_map.columns = df_vendor_map.columns.str.strip()
        # Build list of columns to merge (Raw_Name + all available columns from VENDOR_MAP_COLUMNS)
        merge_cols = ['Raw_Name']
        for col in self.VENDOR_MAP_COLUMNS:
            if col in df_vendor_map.columns:
                merge_cols.append(col)
        print(merge_cols)
        # Merge on vendor name - include all vendor map columns
        df = pd.merge(
            df,
            df_vendor_map[merge_cols].drop_duplicates(subset=['Raw_Name']),
            left_on='vendor_name',
            right_on='Raw_Name',
            how='left'
        )

        # Coalesce by first non-empty value: Standardized_Name > Raw_Name > vendor_name > 'N/A'.
        standardized = _clean_text_col('Standardized_Name')
        raw_name = _clean_text_col('Raw_Name')
        vendor_name = _clean_text_col('vendor_name')
        df['standardized_vendor_name'] = standardized.fillna(raw_name).fillna(vendor_name).fillna('N/A')

        # Rename columns to snake_case for consistency
        column_renames = {
            'Intercompany': 'intercompany',
            'Supplier_Type': 'supplier_type',
            'Location': 'vendor_location',
        }

        for old_col, new_col in column_renames.items():
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)  # When you use inplace=True, the function modifies your original data frame directly and returns nothing (None)

        # Safeguard: ensure N/A vendor rows are retained by Intercompany='N' filters.
        if 'intercompany' not in df.columns:
            df['intercompany'] = ''
        intercompany_blank = df['intercompany'].astype(str).str.strip().isin(['', 'nan', 'None'])
        na_vendor_mask = (
            _clean_text_col('vendor_name').fillna('').str.upper().eq('N/A') |
            _clean_text_col('standardized_vendor_name').fillna('').str.upper().eq('N/A')
        )
        fill_mask = intercompany_blank & na_vendor_mask
        if fill_mask.any():
            df.loc[fill_mask, 'intercompany'] = 'N'

        # Drop helper columns used for join
        df.drop(columns=['Raw_Name', 'Standardized_Name'], inplace=True, errors='ignore')

        # Validate merge match rate
        self._validate_merge(df, "Vendor Standardization",
                             "standardized_vendor_name", _CFG_MIN_VENDOR_MATCH_RATE)

        return df

    def _apply_terms_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes payment terms codes using terms_map.csv."""
        if not os.path.exists(self.terms_map_path):
            print("    Warning: terms_map.csv not found, skipping terms standardization")
            return df

        df_terms_map = pd.read_csv(self.terms_map_path)
        if df_terms_map.empty:
            return df

        df_terms_map.columns = df_terms_map.columns.str.strip()

        # Build lookup dict: Raw_Terms -> Standardized_Terms
        terms_lookup = dict(zip(
            df_terms_map['Raw_Terms'].str.strip(),
            df_terms_map['Standardized_Terms'].str.strip()
        ))

        # Apply mapping; keep original value if not in lookup
        original = df['terms_code'].copy()
        df['terms_code'] = df['terms_code'].map(terms_lookup).fillna(original)

        # Print stats
        changed = (df['terms_code'] != original).sum()
        unique_before = original.dropna().nunique()
        unique_after = df['terms_code'].dropna().nunique()
        print(f"    Standardized {changed:,} terms values ({unique_before} unique -> {unique_after} unique)")

        return df

    def _dedup_item_map(self, df_item_map: pd.DataFrame, merge_cols: list) -> pd.DataFrame:
        """Quality-aware deduplication of item_map before merge.
        
        For duplicate Item_IDs, keeps the row with the highest quality score:
        non-Miscellaneous categories, non-MDO placeholders, higher confidence,
        and more complete spec data are preferred.
        """

        df = df_item_map[merge_cols].copy()

        dedup_keys = ['Item_ID']
        if 'source_system' in df.columns:
            dedup_keys.append('source_system')

        if not df.duplicated(subset=dedup_keys).any():
            return df  # No duplicates, fast path

        df['_quality'] = 0

        # Penalize Miscellaneous/Other
        if 'Category_Level_1' in df.columns:
            df.loc[df['Category_Level_1'] == 'Miscellaneous/Other', '_quality'] -= 50
            df.loc[df['Category_Level_1'].isna(), '_quality'] -= 100

        # Penalize Master Data Only
        if 'Category_Level_3' in df.columns:
            df.loc[df['Category_Level_3'] == 'Master Data Only', '_quality'] -= 40

        # Penalize Uncategorized sub-categories
        if 'Category_Level_2' in df.columns:
            df.loc[df['Category_Level_2'] == 'Uncategorized', '_quality'] -= 30

        # Reward extraction confidence
        if 'extraction_confidence' in df.columns:
            conf = pd.to_numeric(df['extraction_confidence'], errors='coerce').fillna(0)
            df['_quality'] += (conf * 20).astype(int)

        # Reward meaningful description
        if 'Raw_Description' in df.columns:
            has_desc = df['Raw_Description'].notna() & (df['Raw_Description'].str.len() > 10)
            df.loc[has_desc, '_quality'] += 10

        # Sort by quality descending, then keep first (highest quality) per Item_ID
        df = df.sort_values('_quality', ascending=False)
        df = df.drop_duplicates(subset=dedup_keys, keep='first')
        df = df.drop(columns=['_quality'])

        return df

    def _resolve_category_conflicts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Resolve category conflicts for items with the same standardized item_id.
    
        NetSuite and EPICOR share category mappings.
        SYTELINE (and any other ERP) resolves conflicts independently.
        """
        if (
            'item_category_l1' not in df.columns or
            'item_id' not in df.columns or
            'source_system' not in df.columns
        ):
            return df

        cat_cols = [
        'item_category_l1',
        'item_category_l2',
        'item_category_l3',
        'item_category_l4',
        'item_category_l5'
        ]

        # NETSUITE and EPICOR share categories.
        # Other ERPs (e.g. SYTELINE) keep their own categories.
        df['_category_group'] = np.where(
            df['source_system'].isin(['NETSUITE', 'EPICOR']),
            'GENERIC',
            df['source_system']
        )

        # Find groups with conflicting categories
        cat_by_item = (
            df.groupby(['item_id', '_category_group'])['item_category_l1']
            .nunique()
        )
    
        inconsistent_groups = cat_by_item[cat_by_item > 1].index
    
        if len(inconsistent_groups) == 0:
            df.drop(columns=['_category_group'], inplace=True, errors='ignore')
            return df

        print("  Resolving Category Conflicts...")
        fixed_count = 0

        for item_id, category_group in inconsistent_groups:

            mask = (
                (df['item_id'] == item_id) &
                (df['_category_group'] == category_group)
            )

            group = df.loc[mask]

            # Ignore Uncategorized when determining the winning category
            real_cats = group[group['item_category_l1'] != 'Uncategorized']
            if real_cats.empty:
                continue

            best_l1 = real_cats['item_category_l1'].mode().iloc[0]
            best_row = real_cats[real_cats['item_category_l1'] == best_l1].iloc[0]

            for col in cat_cols:
                df.loc[mask, col] = best_row[col]

            fixed_count += 1

        if fixed_count > 0:
            print(f"    Aligned categories for {fixed_count:,} items with conflicting categorizations")

        # Remove temporary helper column
        df.drop(columns=['_category_group'], inplace=True, errors='ignore')

        return df


    def _apply_item_categorization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies item category mapping and enrichment from mapping table."""
        def _fill_category_defaults(frame: pd.DataFrame) -> pd.DataFrame:
            """Normalize empty lower-level categories to literal N/A for reporting."""
            for col in ['item_category_l2', 'item_category_l3', 'item_category_l4', 'item_category_l5']:
                if col in frame.columns:
                    frame[col] = (
                        frame[col]
                        .replace({'': 'N/A', 'nan': 'N/A', 'None': 'N/A'})
                        .fillna('N/A')
                    )
                else:
                    frame[col] = 'N/A'
            if 'item_category_l1' not in frame.columns:
                frame['item_category_l1'] = 'Uncategorized'
            return frame

        if not os.path.exists(self.item_map_path):
            df['category'] = 'Uncategorized'
            df['sub_category'] = ''
            df['item_category_l1'] = 'Uncategorized'
            return _fill_category_defaults(df)

        df_item_map = pd.read_csv(self.item_map_path)
        if df_item_map.empty:
            df['category'] = 'Uncategorized'
            df['sub_category'] = ''
            df['item_category_l1'] = 'Uncategorized'
            return _fill_category_defaults(df)

        # Clean column names
        df_item_map.columns = df_item_map.columns.str.strip()

        # Ensure item_id types match
        if 'item_id' in df.columns:
            df['item_id'] = df['item_id'].astype(str)
        if 'Item_ID' in df_item_map.columns:
            df_item_map['Item_ID'] = df_item_map['Item_ID'].astype(str)

        has_source_key = 'source_system' in df.columns and 'source_system' in df_item_map.columns
        if has_source_key:
            df['source_system'] = df['source_system'].astype(str).str.strip()
            df_item_map['source_system'] = df_item_map['source_system'].astype(str).str.strip()

        # Generic mappings (blank source_system) are allowed only for selected sources.
        generic_fallback_sources = {'NETSUITE', 'EPICOR'}

        # Build list of columns to merge (Item_ID + all available columns from ITEM_MAP_COLUMNS)
        merge_cols = ['Item_ID']
        for col in self.ITEM_MAP_COLUMNS:
            if col in df_item_map.columns:
                merge_cols.append(col)

        # Column rename mapping (original -> snake_case)
        column_renames = {
            'Category_Level_1': 'item_category_l1',
            'Category_Level_2': 'item_category_l2',
            'Category_Level_3': 'item_category_l3',
            'Category_Level_4': 'item_category_l4',
            'Category_Level_5': 'item_category_l5',
            'Raw_Description': 'item_raw_description',
            'analysis_type': 'item_analysis_type',
            'extraction_confidence': 'item_extraction_confidence',
            # Electrical specs
            'spec_type': 'item_spec_type',
            'spec_amps': 'item_spec_amps',
            'spec_voltage': 'item_spec_voltage',
            'spec_phase': 'item_spec_phase',
            'spec_poles': 'item_spec_poles',
            'spec_kaic': 'item_spec_kaic',
            'spec_frame': 'item_spec_frame',
            'spec_kva': 'item_spec_kva',
            'spec_gauge': 'item_spec_gauge',
            'spec_nema': 'item_spec_nema',
            # Raw material specs
            # Item consolidation
            'Canonical_Name': 'item_canonical_name',
            'Canonical_Group_ID': 'item_canonical_group_id',
            # Raw material specs
            'RawMat_Finish': 'rawmat_finish',
            'RawMat_Thickness': 'rawmat_thickness',
            'RawMat_Width': 'rawmat_width',
            'RawMat_Length': 'rawmat_length',
            'RawMat_UoM': 'rawmat_uom',
            'RawMat_Weight_Lbs': 'rawmat_weight_lbs',
        }

        if 'item_id' in df.columns and len(merge_cols) > 1:
            # Quality-aware dedup: best categorization wins (not just first row)
            dedup_cols = merge_cols + (['source_system'] if has_source_key else [])
            
            item_map_dedup = self._dedup_item_map(df_item_map, dedup_cols)

            generic_item_map = None
            generic_source_mask = pd.Series([False] * len(df), index=df.index)
            if has_source_key:
                src_upper = df['source_system'].astype(str).str.upper().str.strip()
                generic_source_mask = src_upper.isin(generic_fallback_sources)
                generic_map_mask = item_map_dedup['source_system'].isna() | (
                    item_map_dedup['source_system'].astype(str).str.strip() == ''
                )
                generic_item_map = item_map_dedup.loc[generic_map_mask].copy()
                if 'source_system' in generic_item_map.columns:
                    generic_item_map.drop(columns=['source_system'], inplace=True)

            # Detect sci-notation IDs (ambiguous, many real IDs collapsed to same string)
            import re
            sci_pattern = re.compile(r'^\d+\.\d+E\+\d+$', re.IGNORECASE)
            sci_notation_ids = set(
                item_map_dedup.loc[
                    item_map_dedup['Item_ID'].str.match(sci_pattern, na=False),
                    'Item_ID'
                ]
            )

            if has_source_key:
                df = pd.merge(
                    df,
                    item_map_dedup,
                    left_on=['item_id', 'source_system'],
                    right_on=['Item_ID', 'source_system'],
                    how='left',
                    suffixes=('', '_map')
                )


                # Fallback pass: allow blank-source item_map matches for NETSUITE/EPICOR only.
                if generic_item_map is not None and not generic_item_map.empty and generic_source_mask.any():
                    pass1_generic_candidates = generic_source_mask & df['Item_ID'].isna()
                    if pass1_generic_candidates.any():
                        fallback_keys = df.loc[pass1_generic_candidates, ['item_id']].copy()
                        fallback_keys['_orig_idx'] = fallback_keys.index
                        fallback_matches = pd.merge(
                            fallback_keys,
                            generic_item_map,
                            left_on='item_id',
                            right_on='Item_ID',
                            how='left'
                        ).set_index('_orig_idx')

                        for col in merge_cols:
                            if col == 'Item_ID':
                                continue
                            if col in df.columns and col in fallback_matches.columns:
                                is_null_or_empty = df[col].isna() | (df[col].astype(str).str.strip() == '')
                                has_new_value = fallback_matches[col].notna()
                                fill_mask = pass1_generic_candidates & is_null_or_empty & has_new_value
                                if fill_mask.any():
                                    df.loc[fill_mask, col] = fallback_matches.loc[fill_mask, col]
            else:
                df = pd.merge(
                    df,
                    item_map_dedup,
                    left_on='item_id',
                    right_on='Item_ID',
                    how='left',
                    suffixes=('', '_map')
                )

            # For rows that matched an ambiguous sci-notation ID, clear Pass 1 results
            # so Pass 2 (item_name_mpn match) can find the correct categorization
            if sci_notation_ids:
                matched_sci = df['Item_ID'].isin(sci_notation_ids) & df['Item_ID'].notna()
                if matched_sci.any():
                    sci_count = matched_sci.sum()
                    print(f"    [Note] {sci_count:,} rows matched ambiguous sci-notation IDs - deferring to Pass 2")
                    for col in merge_cols:
                        if col == 'Item_ID':
                            continue
                        if col in df.columns:
                            df.loc[matched_sci, col] = None

            # Track which rows got matched in pass 1 (with actual data, not just a match)
            # A row needs pass 2 if: not matched OR matched but key columns are null
            has_rawmat_data = df['RawMat_Weight_Lbs'].notna() if 'RawMat_Weight_Lbs' in df.columns else pd.Series([False] * len(df))

            # Drop Item_ID from first merge
            df.drop(columns=['Item_ID'], inplace=True, errors='ignore')

            # Pass 2: For rows without rawmat data, try matching on item_name_mpn
            # (works for NETSUITE where item_id is numeric but item_name_mpn has the item name)
            if 'item_name_mpn' in df.columns:
                needs_pass2 = ~has_rawmat_data
                needs_pass2_count = needs_pass2.sum()

                if needs_pass2_count > 0:
                    # Merge item_name_mpn against Item_ID for rows needing pass 2
                    pass2_key_cols = ['item_name_mpn'] + (['source_system'] if has_source_key else [])

                    df_needs_pass2_keys = df.loc[needs_pass2, pass2_key_cols].copy()

                    df_needs_pass2_keys['_orig_idx'] = df_needs_pass2_keys.index
                
                    if has_source_key:
                        df_matched_by_name = pd.merge(
                            df_needs_pass2_keys,
                            item_map_dedup,
                            left_on=['item_name_mpn', 'source_system'],
                            right_on=['Item_ID', 'source_system'],
                            how='left'
                        ).set_index('_orig_idx')


                        # Generic fallback for pass 2 (item_name_mpn), only for NETSUITE/EPICOR rows.
                        if generic_item_map is not None and not generic_item_map.empty:
                            pass2_generic_candidates = needs_pass2 & generic_source_mask
                            if pass2_generic_candidates.any():
                                fallback_name_keys = df.loc[pass2_generic_candidates, ['item_name_mpn']].copy()
                                fallback_name_keys['_orig_idx'] = fallback_name_keys.index
                                fallback_name_matches = pd.merge(
                                    fallback_name_keys,
                                    generic_item_map,
                                    left_on='item_name_mpn',
                                    right_on='Item_ID',
                                    how='left'
                                ).set_index('_orig_idx')

                                for col in merge_cols:
                                    if col == 'Item_ID':
                                        continue
                                    if col in df_matched_by_name.columns and col in fallback_name_matches.columns:
                                        source_null = df_matched_by_name[col].isna() | (df_matched_by_name[col].astype(str).str.strip() == '')
                                        df_matched_by_name.loc[source_null, col] = fallback_name_matches.loc[source_null, col]
                    else:
                        df_matched_by_name = pd.merge(
                            df_needs_pass2_keys,
                            item_map_dedup,
                            left_on='item_name_mpn',
                            right_on='Item_ID',
                            how='left'
                        ).set_index('_orig_idx')

                    # Only update columns where Pass 1 left nulls (don't overwrite good data)
                    # This prevents conflicting item_map entries from overwriting correct categories
                    for col in merge_cols:
                        if col == 'Item_ID':
                            continue
                        if col in df.columns and col in df_matched_by_name.columns:
                            # Only fill where current value is null or empty string
                            is_null_or_empty = df[col].isna() | (df[col].astype(str).str.strip() == '')
                            has_new_value = df_matched_by_name[col].notna()
                            mask = is_null_or_empty & has_new_value
                            if mask.any():
                                df.loc[mask, col] = df_matched_by_name.loc[mask, col]

            # Rename columns to snake_case for consistency
            for old_col, new_col in column_renames.items():
                if old_col in df.columns:
                    df.rename(columns={old_col: new_col}, inplace=True)

            # Handle suffixed columns if they exist (from merge conflicts)
            for old_col, new_col in column_renames.items():
                col_map = f"{old_col}_map"
                if col_map in df.columns:
                    df[new_col] = df[col_map]
                    df.drop(columns=[col_map], inplace=True)

        # Fill defaults for category columns
        df['item_category_l1'] = df.get('item_category_l1', pd.Series(['Uncategorized'] * len(df))).fillna('Uncategorized')
        df = _fill_category_defaults(df)

        # Post-merge diagnostics and validation
        categorized = (df['item_category_l1'] != 'Uncategorized')
        cat_count = categorized.sum()
        uncat_count = (~categorized).sum()
        print(f"    Categorized rows:   {cat_count:,}")
        print(f"    Uncategorized rows: {uncat_count:,}")

        # Validate item categorization rate
        cat_rate = cat_count / len(df) if len(df) > 0 else 0
        print(f"    Item categorization rate: {cat_rate:.1%}")
        if cat_rate < _CFG_MIN_ITEM_MATCH_RATE:
            raise DataQualityError(
                f"Item categorization rate {cat_rate:.1%} is below threshold "
                f"{_CFG_MIN_ITEM_MATCH_RATE:.0%}. Check item_map.csv enrichment."
            )

        return df

    def _apply_description_fallback(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills in placeholder/blank item descriptions using a fallback chain.

        Fallback order:
        1. item_display_name
        2. item_raw_description
        3. line_description
        4. item_name_mpn (last resort)

        Some items have placeholder descriptions like '[FILL IN DESCRIPTION]' from
        the source system. This method replaces those with more descriptive values.
        """
        if 'item_description' not in df.columns:
            return df

        # Define placeholder patterns that should be replaced
        placeholder_patterns = ['[FILL IN DESCRIPTION]', '[FILL IN]']

        def needs_fallback(col):
            """Check if a column value needs to be replaced."""
            return (
                col.isna() |
                col.isin(placeholder_patterns) |
                (col.astype(str).str.strip() == '') |
                (col.astype(str) == 'nan')
            )

        def has_valid_value(col):
            """Check if a column has a valid non-empty value."""
            return (
                col.notna() &
                (~col.astype(str).isin(['nan', ''])) &
                (col.astype(str).str.strip() != '')
            )

        # Fallback chain: try each source in order
        fallback_sources = [
            ('item_display_name', 'item_display_name'),
            ('item_raw_description', 'item_raw_description'),
            ('line_description', 'line_description'),
            ('item_name_mpn', 'item_name_mpn'),
        ]

        total_replaced = 0
        for source_col, source_name in fallback_sources:
            if source_col not in df.columns:
                continue

            # Find rows that still need a description AND have a valid source value
            still_needs = needs_fallback(df['item_description'])
            has_source = has_valid_value(df[source_col])
            mask = still_needs & has_source

            replaced_count = mask.sum()
            print(mask)
            if replaced_count > 0:
                df.loc[mask, 'item_description'] = df.loc[mask, source_col]
                print(f"    Filled {replaced_count:,} descriptions from {source_name}")
                total_replaced += replaced_count

        if total_replaced > 0:
            print(f"    Total descriptions filled: {total_replaced:,}")

        return df

    def _build_item_name(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build a unified item_name column from the best available descriptive source.

        Priority chain (first non-empty wins):
        1. item_display_name   - NetSuite friendly name (best when available)
        2. item_description    - Source description (after fallback chain applied)
        3. item_raw_description - AI-enriched description from item_map
        4. line_description    - Epicor line override
        5. item_name_mpn       - Part number (last resort, always available)

        Runs AFTER _apply_description_fallback() and _apply_item_categorization()
        so all source columns are fully populated.
        """
        placeholders = {'[FILL IN DESCRIPTION]', '[FILL IN]', '', 'nan', 'None'}

        def is_valid(series):
            """Return boolean mask: True where value is non-null and not a placeholder."""
            return (
                series.notna() &
                (~series.astype(str).str.strip().isin(placeholders)) &
                (series.astype(str).str.strip() != '')
            )

        # Start with NaN and track which source was used
        df['item_name'] = pd.Series([np.nan] * len(df), index=df.index)
        source_counts = {}

        # Priority sources (highest priority first)
        priority_sources = [
            'item_display_name',
            'item_description',
            'item_raw_description',
            'line_description',
            'item_name_mpn',
        ]

        # Apply in forward order (highest priority first), only filling where still empty
        for source_col in priority_sources:
            if source_col not in df.columns:
                continue
            needs_name = ~is_valid(df['item_name'])
            has_source = is_valid(df[source_col])
            mask = needs_name & has_source
            count = mask.sum()
            if count > 0:
                df.loc[mask, 'item_name'] = df.loc[mask, source_col].astype(str).str.strip()
                source_counts[source_col] = count

        # Final fallback: if still empty, use item_name_mpn unconditionally
        still_empty = ~is_valid(df['item_name'])
        if still_empty.any() and 'item_name_mpn' in df.columns:
            df.loc[still_empty, 'item_name'] = df.loc[still_empty, 'item_name_mpn']
            source_counts['item_name_mpn (fallback)'] = still_empty.sum()

        # Log summary
        filled = is_valid(df['item_name']).sum()
        print(f"    Built item_name for {filled:,} / {len(df):,} rows")
        for src, count in source_counts.items():
            print(f"      from {src}: {count:,}")

        return df

    def _fill_blank_item_ids_for_reporting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace blank item_id values with literal N/A for reporting and slicers."""
        if 'item_id' not in df.columns:
            return df

        blank_mask = df['item_id'].isna() | (df['item_id'].astype(str).str.strip() == '')
        blank_count = blank_mask.sum()
        if blank_count > 0:
            df.loc[blank_mask, 'item_id'] = 'N/A'
            print(f"    Replaced {blank_count:,} blank item_id values with N/A for reporting")

        return df

    def _fill_blank_po_numbers_for_reporting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace blank po_number values with literal N/A for reporting and joins."""
        if 'po_number' not in df.columns:
            return df

        po_number_normalized = df['po_number'].astype(str).str.strip()
        blank_mask = (
            df['po_number'].isna() |
            po_number_normalized.isin(['', 'nan', 'None'])
        )
        blank_count = blank_mask.sum()
        if blank_count > 0:
            df.loc[blank_mask, 'po_number'] = 'N/A'
            print(f"    Replaced {blank_count:,} blank po_number values with N/A for reporting")

        return df

    def _fill_blank_item_labels_for_reporting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Set item_name/item_short_name to N/A when all source description fields are blank."""
        source_cols = [
            'item_description',
            'item_display_name',
            'item_name_mpn',
            'line_description',
        ]

        present_source_cols = [c for c in source_cols if c in df.columns]
        if not present_source_cols:
            return df

        all_sources_blank = pd.Series(True, index=df.index)
        for col in present_source_cols:
            all_sources_blank &= (
                df[col].isna() |
                (df[col].astype(str).str.strip() == '') |
                (df[col].astype(str).str.lower() == 'nan')
            )

        if 'item_name' in df.columns:
            name_blank = (
                df['item_name'].isna() |
                (df['item_name'].astype(str).str.strip() == '') |
                (df['item_name'].astype(str).str.lower() == 'nan')
            )
            fill_name_mask = all_sources_blank & name_blank
            fill_name_count = fill_name_mask.sum()
            if fill_name_count > 0:
                df.loc[fill_name_mask, 'item_name'] = 'N/A'
                print(f"    Replaced {fill_name_count:,} blank item_name values with N/A for reporting")

        if 'item_short_name' in df.columns:
            short_blank = (
                df['item_short_name'].isna() |
                (df['item_short_name'].astype(str).str.strip() == '') |
                (df['item_short_name'].astype(str).str.lower() == 'nan')
            )
            fill_short_mask = all_sources_blank & short_blank
            fill_short_count = fill_short_mask.sum()
            if fill_short_count > 0:
                df.loc[fill_short_mask, 'item_short_name'] = 'N/A'
                print(f"    Replaced {fill_short_count:,} blank item_short_name values with N/A for reporting")

        return df

    def _calculate_rawmat_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate normalized weight and price metrics for raw materials.

        Handles UOM inconsistencies:
        - Pound UOM: quantity is already in pounds, unit_cost is per pound
        - FT/EA/EACH UOM: quantity is in feet, rawmat_weight_lbs is per 12-ft bar,
          unit_cost is per 12-ft bar

        Adds:
        - rawmat_total_weight_lbs: Total weight for the line item
        - rawmat_price_per_lb: Normalized price per pound
        """
        # Initialize columns
        df['rawmat_total_weight_lbs'] = np.nan
        df['rawmat_price_per_lb'] = np.nan

        # Only process rows with raw material weight data
        has_rawmat = df['rawmat_weight_lbs'].notna()

        if not has_rawmat.any():
            return df

        # Get quantity (prefer received, fallback to total)
        qty = df['received_quantity'].fillna(df['total_quantity'])

        # UOM = Pound: quantity IS in pounds
        pound_mask = has_rawmat & (df['uom_internal'] == 'Pound')
        df.loc[pound_mask, 'rawmat_total_weight_lbs'] = qty[pound_mask]
        df.loc[pound_mask, 'rawmat_price_per_lb'] = df.loc[pound_mask, 'unit_cost']

        # UOM = FT/EA/EACH: quantity is in feet or each (bars)
        # rawmat_weight_lbs is per 12-foot bar
        ft_mask = has_rawmat & (df['uom_internal'].isin(['FT', 'EA', 'EACH']))
        # Convert feet to bars (qty / 12), then multiply by weight per bar
        df.loc[ft_mask, 'rawmat_total_weight_lbs'] = (qty[ft_mask] / 12) * df.loc[ft_mask, 'rawmat_weight_lbs']

        # unit_cost is per 12-ft bar, divide by bar weight to get per-pound
        # Guard against division by zero - only divide where weight > 0
        valid_weight_mask = ft_mask & (df['rawmat_weight_lbs'] > 0)
        zero_weight_mask = ft_mask & ((df['rawmat_weight_lbs'] == 0) | df['rawmat_weight_lbs'].isna())

        df.loc[valid_weight_mask, 'rawmat_price_per_lb'] = (
            df.loc[valid_weight_mask, 'unit_cost'] / df.loc[valid_weight_mask, 'rawmat_weight_lbs']
        )

        # Log zero-weight cases
        zero_weight_count = zero_weight_mask.sum()
        if zero_weight_count > 0:
            print(f"    [Warning] {zero_weight_count} raw material rows have zero/null weight - price_per_lb set to NaN")

        return df

    def _apply_copper_price_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Join copper market price index for copper products.
        Only populates for rows where item_category_l2 == 'Copper Products'.
        """
        copper_index_path = os.path.join(
            os.path.dirname(self.item_map_path),
            "copper_price_index.csv"
        )

        if not os.path.exists(copper_index_path):
            print("  Copper price index not found, skipping...")
            df['copper_market_price_per_lb'] = np.nan
            return df

        print("  Applying Copper Price Index...")
        copper_df = pd.read_csv(copper_index_path)

        # Validate copper_df has unique year_month to prevent Cartesian product
        if copper_df['year_month'].duplicated().any():
            dup_count = copper_df['year_month'].duplicated().sum()
            print(f"    [Warning] Copper price index has {dup_count} duplicate year_month entries - deduplicating")
            copper_df = copper_df.drop_duplicates(subset=['year_month'], keep='first')

        # Track row count before join to detect Cartesian product
        row_count_before = len(df)

        # Join on order_year_month
        df = df.merge(
            copper_df[['year_month', 'copper_price_per_lb']],
            left_on='order_year_month',
            right_on='year_month',
            how='left'
        )

        # Validate row count didn't increase (would indicate Cartesian product)
        row_count_after = len(df)
        if row_count_after > row_count_before:
            raise ValueError(
                f"Copper price join caused row explosion! Before: {row_count_before:,}, After: {row_count_after:,}. "
                f"Check for duplicate year_month values in copper_price_index.csv"
            )

        # Rename and only keep for copper products
        df['copper_market_price_per_lb'] = np.where(
            df['item_category_l2'] == 'Copper Products',
            df['copper_price_per_lb'],
            np.nan
        )

        # Clean up
        df.drop(columns=['year_month', 'copper_price_per_lb'], inplace=True, errors='ignore')

        copper_rows = df['copper_market_price_per_lb'].notna().sum()
        print(f"    Applied copper index to {copper_rows:,} rows")

        return df

    def _apply_reporting_date_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter transactions to reporting date range using inclusive OR logic.

        Includes records where EITHER:
        - order_date >= REPORTING_START_DATE (for open orders and historical receipts), OR
        - receipt_date >= REPORTING_START_DATE (for receipts)

        This supports dual reporting perspectives:
        - Order date view: Shows when POs were placed
        - Receipt date view: Shows when goods were received

        Example: A 2023 order received in 2024 is INCLUDED (receipt_date qualifies)
        """
        print(f"  Applying Reporting Date Filter (>= {self.REPORTING_START_DATE})...")
        print(f"    Filter logic: (order_date >= cutoff) OR (receipt_date >= cutoff)")

        initial_count = len(df)
        reporting_start = pd.to_datetime(self.REPORTING_START_DATE)

        # Build inclusive OR filter
        # Include if order_date qualifies OR receipt_date qualifies
        order_date_qualifies = (df['order_date'] >= reporting_start)
        receipt_date_qualifies = (df['receipt_date'] >= reporting_start) & df['receipt_date'].notna()

        filter_mask = order_date_qualifies | receipt_date_qualifies
        df_filtered = df[filter_mask].copy()

        filtered_count = len(df_filtered)
        excluded_count = initial_count - filtered_count

        print(f"    Rows before filter: {initial_count:,}")
        print(f"    Rows after filter:  {filtered_count:,}")
        print(f"    Rows excluded:      {excluded_count:,} ({excluded_count/initial_count*100:.1f}%)")

        # Show which filter criteria matched
        order_only = (order_date_qualifies & ~receipt_date_qualifies).sum()
        receipt_only = (receipt_date_qualifies & ~order_date_qualifies).sum()
        both = (order_date_qualifies & receipt_date_qualifies).sum()

        print(f"\n    Filter criteria breakdown:")
        print(f"      Qualified by order_date only:    {order_only:6,} (open orders + old receipts)")
        print(f"      Qualified by receipt_date only:  {receipt_only:6,} (2023 orders, 2024+ receipts)")
        print(f"      Qualified by both dates:         {both:6,}")

        # Breakdown by source system
        if 'source_system' in df.columns:
            print(f"\n    Breakdown by source system:")
            for source in df['source_system'].unique():
                before = (df['source_system'] == source).sum()
                after = (df_filtered['source_system'] == source).sum()
                excluded = before - after
                print(f"      {source:12} | Before: {before:6,} | After: {after:6,} | Excluded: {excluded:6,}")

        # Show final date ranges for both fields
        if not df_filtered.empty:
            if 'order_date' in df_filtered.columns:
                min_order = df_filtered['order_date'].min()
                max_order = df_filtered['order_date'].max()
                print(f"\n    Order date range:   {min_order.strftime('%Y-%m-%d')} to {max_order.strftime('%Y-%m-%d')}")

            if 'receipt_date' in df_filtered.columns and df_filtered['receipt_date'].notna().any():
                min_receipt = df_filtered['receipt_date'].min()
                max_receipt = df_filtered['receipt_date'].max()
                print(f"    Receipt date range: {min_receipt.strftime('%Y-%m-%d')} to {max_receipt.strftime('%Y-%m-%d')}")

        return df_filtered

    def _add_time_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds time-based metrics: lead time, days until due, days since order, receipt variance."""
        today = pd.Timestamp.now().normalize()

        if 'order_date' in df.columns and 'due_date' in df.columns:
            df['lead_time_days'] = (df['due_date'] - df['order_date']).dt.days
        else:
            df['lead_time_days'] = None

        if 'due_date' in df.columns:
            # Fallback chain: due_date > promise_date
            effective_due = df['due_date'].copy()
            if 'promise_date' in df.columns:
                effective_due = effective_due.fillna(df['promise_date'])
            df['days_until_due'] = (effective_due - today).dt.days
        else:
            df['days_until_due'] = None

        if 'order_date' in df.columns:
            df['days_since_order'] = (today - df['order_date']).dt.days
        else:
            df['days_since_order'] = None

        if 'received_quantity' in df.columns and 'total_quantity' in df.columns:
            df['receipt_variance_qty'] = df['received_quantity'] - df['total_quantity']
            df['receipt_variance_pct'] = np.where(
                df['total_quantity'] > 0,
                (df['received_quantity'] - df['total_quantity']) / df['total_quantity'] * 100,
                0
            )
        else:
            df['receipt_variance_qty'] = None
            df['receipt_variance_pct'] = None

        return df

    def _add_status_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds status flags using the promise-date + grace-period rule."""

        # Use analysis_date as the reporting/as-of date when it is available.
        # Fall back to the current date only if analysis_date has not been computed.
        if 'analysis_date' in df.columns:
            as_of_date = pd.to_datetime(df['analysis_date'], errors='coerce')
        else:
            as_of_date = pd.Series(pd.Timestamp.now().normalize(), index=df.index)

        promise = (
            pd.to_datetime(df['promise_date'], errors='coerce')
            if 'promise_date' in df.columns
            else pd.Series(pd.NaT, index=df.index)
        )

        if 'open_quantity' in df.columns:
            qty_open = pd.to_numeric(df['open_quantity'], errors='coerce').fillna(0)
        else:
            qty_open = pd.Series(0, index=df.index, dtype='float64')

        # Open orders are overdue only after promise_date + 3 days.
        # Do not fall back to due_date for the contractual OTD rule.
        grace_boundary = promise + pd.Timedelta(days=self.GRACE_PERIOD_DAYS)
        df['is_overdue'] = (
            promise.notna()
            & as_of_date.notna()
            & (as_of_date > grace_boundary)
            & (qty_open > 0)
        )

        # Preserve the existing row-level is_fully_received field.
        # The new OTD-specific completion status is calculated separately at PO-line grain.
        if 'open_quantity' in df.columns:
            df['is_fully_received'] = pd.to_numeric(
                df['open_quantity'], errors='coerce'
            ).fillna(0) <= 0
        elif 'total_quantity' in df.columns and 'received_quantity' in df.columns:
            total_qty = pd.to_numeric(df['total_quantity'], errors='coerce').fillna(0)
            received_qty = pd.to_numeric(df['received_quantity'], errors='coerce').fillna(0)
            df['is_fully_received'] = received_qty >= total_qty
        else:
            df['is_fully_received'] = None

        return df

    def _add_otd_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate one OTD result per PO line without changing fact-table grain.

        Physical Receipt/Open Order rows are preserved. No synthetic rows are created.
        For each PO line, an existing Open Order row is the OTD representative while
        the line is incomplete; once fully received, the latest existing receipt row
        is the OTD representative.
        """
        key_cols = ['source_system', 'po_number', 'po_line', 'po_release_num']
        for col in key_cols:
            if col not in df.columns:
                df[col] = ''

        for col in ['promise_date', 'receipt_date', 'analysis_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        key_frame = df[key_cols].copy().fillna('')
        for col in key_cols:
            key_frame[col] = key_frame[col].astype(str).str.strip()
        df['otd_line_key'] = key_frame.astype(str).agg('|'.join, axis=1)

        df['final_receipt_date'] = pd.NaT
        df['delta_of_promise_date_vs_receipt_date'] = np.nan
        df['otd_is_fully_received'] = False
        df['is_otd_representative'] = False
        df['on_time_vs_promise_flag'] = None
        df['on_time_flag'] = None

        if df.empty:
            return df

        tt = df.get('transaction_type', pd.Series('', index=df.index)).astype(str).str.strip().str.lower()
        is_receipt = tt.eq('receipts')
        is_open = tt.eq('open orders')

        def num_col(name):
            return pd.to_numeric(df[name], errors='coerce') if name in df.columns else pd.Series(np.nan, index=df.index)

        open_qty = num_col('open_quantity')
        total_qty = num_col('total_quantity')
        received_qty = num_col('received_quantity').fillna(0)

        for key, idx in df.groupby('otd_line_key', sort=False).groups.items():
            idx = list(idx)
            g = df.loc[idx]
            receipt_idx = [i for i in idx if is_receipt.loc[i] and pd.notna(df.at[i, 'receipt_date'])]
            open_idx = [i for i in idx if is_open.loc[i]]

            # Existing Open Order row, if present, is authoritative for current open qty.
            existing_open_qty = open_qty.loc[open_idx].dropna() if open_idx else pd.Series(dtype=float)
            if len(existing_open_qty):
                remaining_qty = float(existing_open_qty.max())
                fully_received = remaining_qty <= 0
            else:
                # For partial receipts where no Open Order row exists, derive only the
                # completion state from existing quantities. Do NOT create a new row.
                ordered_values = total_qty.loc[idx].dropna()
                ordered_qty = float(ordered_values.max()) if len(ordered_values) else np.nan
                received_total = float(received_qty.loc[receipt_idx].sum()) if receipt_idx else 0.0
                if pd.notna(ordered_qty):
                    remaining_qty = ordered_qty - received_total
                    fully_received = remaining_qty <= 0
                else:
                    # If ordered quantity is unavailable, a receipt-only line cannot
                    # safely be declared partial; preserve existing row semantics.
                    fully_received = bool(receipt_idx)
                    remaining_qty = np.nan

            promise_values = pd.to_datetime(g['promise_date'], errors='coerce').dropna()
            promise_date = promise_values.max() if len(promise_values) else pd.NaT

            final_receipt = pd.NaT
            representative_idx = None

            # IMPORTANT OTD representative rule:
            #   - If the PO line is still open/partial, the EXISTING Open Order
            #     row represents the OTD item. Receipt rows are never the OTD
            #     representative while the line is incomplete.
            #   - If the PO line is fully received, the latest receipt row
            #     represents the OTD item. Any existing Open Order row is 0.
            #   - No synthetic rows are created. If an incomplete line has no
            #     existing Open Order row, there is no row we can mark as the
            #     representative; this is intentionally surfaced rather than
            #     incorrectly assigning OTD to a receipt row.
            if fully_received:
                if receipt_idx:
                    receipt_dates = pd.to_datetime(df.loc[receipt_idx, 'receipt_date'], errors='coerce')
                    max_receipt = receipt_dates.max()
                    candidates = receipt_dates[receipt_dates == max_receipt].index.tolist()
                    representative_idx = candidates[-1]
                    final_receipt = max_receipt
            else:
                if open_idx:
                    representative_idx = open_idx[-1]

            # OTD status: representative PO-line result only, then stamp it to all
            # physical rows so Power BI can filter to is_otd_representative = 1.
            status = None
            delta = np.nan
            if pd.notna(promise_date):
                boundary = promise_date + pd.Timedelta(days=self.GRACE_PERIOD_DAYS)
                if fully_received and pd.notna(final_receipt):
                    status = 'On Time' if final_receipt <= boundary else 'Late'
                    delta = (promise_date - final_receipt).days
                else:
                    # No final receipt yet. Use analysis_date as the reporting/as-of date.
                    analysis_values = pd.to_datetime(g['analysis_date'], errors='coerce').dropna() if 'analysis_date' in g.columns else pd.Series(dtype='datetime64[ns]')
                    if len(analysis_values) and analysis_values.max() > boundary:
                        status = 'Late'
                    else:
                        status = None

            df.loc[idx, 'otd_is_fully_received'] = fully_received
            df.loc[idx, 'final_receipt_date'] = final_receipt
            df.loc[idx, 'delta_of_promise_date_vs_receipt_date'] = delta

            # Only the representative row carries the OTD status. This keeps
            # the physical receipt/open-order rows intact while ensuring there
            # is exactly one OTD result per PO line in Power BI.
            if representative_idx is not None:
                df.at[representative_idx, 'is_otd_representative'] = True

            # Populate the PO-line OTD status on all physical rows.
            df.at[representative_idx, 'on_time_vs_promise_flag'] = status
            df.at[representative_idx, 'on_time_flag'] = status

        # Keep Due Date operational classification unchanged. Early may still exist here;
        # Early is removed only from Promise-Date OTD.
        if 'receipt_date' in df.columns and 'due_date' in df.columns:
            receipt_dt = pd.to_datetime(df['receipt_date'], errors='coerce')
            due_dt = pd.to_datetime(df['due_date'], errors='coerce')
            both = receipt_dt.notna() & due_dt.notna()
            early_boundary = due_dt - pd.Timedelta(days=14)
            late_boundary = due_dt + pd.Timedelta(days=self.GRACE_PERIOD_DAYS)
            due_result = pd.Series(None, index=df.index, dtype='object')
            due_result[both & (receipt_dt < early_boundary)] = 'Early'
            due_result[both & (receipt_dt >= early_boundary) & (receipt_dt <= late_boundary)] = 'On Time'
            due_result[both & (receipt_dt > late_boundary)] = 'Late'
            df['on_time_vs_due_flag'] = due_result
        else:
            df['on_time_vs_due_flag'] = None

        # Operational overdue flag: only an actually open quantity can be overdue.
        if 'open_quantity' in df.columns:
            oq = pd.to_numeric(df['open_quantity'], errors='coerce').fillna(0)
            promise = pd.to_datetime(df.get('promise_date'), errors='coerce')
            analysis = pd.to_datetime(df.get('analysis_date'), errors='coerce')
            df['is_overdue'] = (
                promise.notna() & analysis.notna() & (oq > 0) &
                (analysis > promise + pd.Timedelta(days=self.GRACE_PERIOD_DAYS))
            )
        else:
            df['is_overdue'] = False

        df['is_late_or_overdue'] = (
            (df['on_time_vs_promise_flag'] == 'Late') |
            df['is_overdue'].fillna(False).astype(bool)
        )
        return df

    def _add_calculated_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds all dashboard-ready calculated metrics."""
        df = self._add_time_metrics(df)
        df = self._calculate_quantity_amounts(df)

        # Spend Bucket (use open_plus_received_amount as the total line value)
        if 'open_plus_received_amount' in df.columns:
            df['spend_bucket'] = self._categorize_spend(df['open_plus_received_amount'])
        else:
            df['spend_bucket'] = 'Unknown'

        return df

    def _categorize_spend(self, amount_series: pd.Series) -> pd.Series:
        """Categorizes spend amounts into buckets."""
        result = pd.Series(['Unknown'] * len(amount_series), index=amount_series.index)

        for low, high, label in self.SPEND_BUCKETS:
            mask = (amount_series >= low) & (amount_series < high)
            result[mask] = label

        # Handle negatives and nulls
        result[amount_series < 0] = 'Negative'
        result[amount_series.isna()] = 'Unknown'

        return result

    def _correct_unit_costs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect and correct EPICOR 1000x unit_cost issue (mills vs dollars).

        Some EPICOR records store unit_cost in mills (1/1000 of a dollar).
        Detected by comparing unit_cost to implied cost (extended_amount / quantity).
        Correction: divide by 1000 when ratio is between 500x-2000x.

        Adds audit columns:
        - _original_unit_cost: Preserved original value
        - _unit_cost_correction: Description of correction applied
        - calculated_price: Corrected unit_cost
        """
        df['_original_unit_cost'] = df['unit_cost'].copy()
        df['_unit_cost_correction'] = None
        df['_corrected_unit_cost'] = df['unit_cost'].copy()

        # For EPICOR Receipts, check if unit_cost is ~1000x the implied cost
        ep_rcpt_mask = (df['source_system'] == 'EPICOR') & (df['transaction_type'] == 'Receipts')
        if ep_rcpt_mask.any() and 'extended_amount' in df.columns:
            # Use received_quantity if UOMs match, else use vendor_quantity
            qty_for_check = df['received_quantity'].where(
                df['uom_internal'] == df['uom_purchasing'],
                df['vendor_quantity']
            )
            qty_for_check = qty_for_check.fillna(df['received_quantity'])
            implied_cost = df['extended_amount'] / qty_for_check.replace(0, np.nan)

            # Detect ~1000x ratio (widened range: 500x to 2000x to catch edge cases)
            cost_ratio = df['unit_cost'] / implied_cost.replace(0, np.nan)
            is_1000x = ep_rcpt_mask & (cost_ratio > 500) & (cost_ratio < 2000)

            df.loc[is_1000x, '_corrected_unit_cost'] = df.loc[is_1000x, 'unit_cost'] / 1000
            df.loc[is_1000x, '_unit_cost_correction'] = '1000x_mills_to_dollars'

            corrected_count = is_1000x.sum()
            if corrected_count > 0:
                print(f"    [Data Quality Fix] Corrected {corrected_count} RECEIPT rows with 1000x unit_cost issue")
                sample_pos = df.loc[is_1000x, 'po_number'].head(5).tolist()
                print(f"      Sample POs corrected: {sample_pos}")

        # Also detect 1000x issue for EPICOR Open Orders
        ep_open = (df['source_system'] == 'EPICOR') & (df['transaction_type'] == 'Open Orders')
        if ep_open.any() and 'extended_amount' in df.columns:
            same_uom_open = ep_open & ((df['uom_internal'] == df['uom_purchasing']) | df['uom_purchasing'].isna())
            if same_uom_open.any():
                implied_cost_open = df['extended_amount'] / df['total_quantity'].replace(0, np.nan)
                cost_ratio_open = df['unit_cost'] / implied_cost_open.replace(0, np.nan)
                is_1000x_open = same_uom_open & (cost_ratio_open > 500) & (cost_ratio_open < 2000)
                df.loc[is_1000x_open, '_corrected_unit_cost'] = df.loc[is_1000x_open, 'unit_cost'] / 1000
                df.loc[is_1000x_open, '_unit_cost_correction'] = '1000x_mills_to_dollars'

                corrected_open = is_1000x_open.sum()
                if corrected_open > 0:
                    print(f"    [Data Quality Fix] Corrected {corrected_open} OPEN_ORDER rows with 1000x unit_cost issue")
                    sample_pos = df.loc[is_1000x_open, 'po_number'].head(5).tolist()
                    print(f"      Sample POs corrected: {sample_pos}")

        df['calculated_price'] = df['_corrected_unit_cost']
        return df

    def _calculate_quantity_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the 6-column quantity/amount structure for the PO fact table.

        Columns:
        1. open_quantity - Already calculated in pre_cleaner (OPEN_ORDER) or 0 (RECEIPT)
        2. open_amount - open_quantity × corrected_unit_cost
        3. received_quantity - From source (unchanged)
        4. received_amount - received_quantity × corrected_unit_cost
        5. open_plus_received_quantity - open_quantity + received_quantity
        6. open_plus_received_amount - open_amount + received_amount

        Business Rules:
        - OPEN_ORDER: All 6 columns populated meaningfully
        - RECEIPT: open_quantity/open_amount = 0, received columns populated
        - For RECEIPT rows, received_amount uses extended_amount from ERP when available
        """
        print("  Calculating Quantity/Amount Metrics...")

        # Correct unit costs first (1000x mills-to-dollars issue)
        df = self._correct_unit_costs(df)
        df['_amount_source'] = 'calculated'

        # --- Set Receipts open_quantity to 0 ---
        is_receipt = df['transaction_type'] == 'Receipts'
        df.loc[is_receipt, 'open_quantity'] = 0

        # --- Calculate quantity/amount columns ---
        # Use corrected unit cost and ensure numeric types
        corrected_cost = pd.to_numeric(df['_corrected_unit_cost'], errors='coerce').fillna(0)
        open_qty = pd.to_numeric(df['open_quantity'], errors='coerce').fillna(0)
        received_qty = pd.to_numeric(df['received_quantity'], errors='coerce').fillna(0)

        # 1. open_amount
        df['open_amount'] = open_qty * corrected_cost

        # 2. received_amount
        # For OPEN_ORDER: calculate from received_qty × corrected_cost
        # For RECEIPT: use source extended_amount (the actual transaction value from ERP)
        df['received_amount'] = received_qty * corrected_cost  # Default calculation

        # Override for RECEIPT rows: use extended_amount from source system
        # This is the actual receipt value recorded by the ERP, which may include
        # discounts, negotiated pricing, or other adjustments not reflected in unit_cost
        if 'extended_amount' in df.columns:
            ext_amt = pd.to_numeric(df['extended_amount'], errors='coerce')
            valid_ext = is_receipt & ext_amt.notna() & (ext_amt != 0)
            df.loc[valid_ext, 'received_amount'] = ext_amt[valid_ext]
            df.loc[valid_ext, '_amount_source'] = 'extended_amount'
            override_count = valid_ext.sum()
            if override_count > 0:
                print(f"    [RECEIPT Fix] Used extended_amount for {override_count:,} RECEIPT rows")

                # Log significant discrepancies between calculated and extended_amount
                calc_amt = received_qty * corrected_cost
                diff_pct = abs(calc_amt - ext_amt) / ext_amt.replace(0, np.nan) * 100
                significant_diff = valid_ext & (diff_pct > 10)
                if significant_diff.sum() > 0:
                    print(f"    [Warning] {significant_diff.sum():,} RECEIPT rows have >10% difference between calculated and extended_amount")

        # 3. open_plus_received_quantity (NEW)
        df['open_plus_received_quantity'] = open_qty + received_qty

        # 4. open_plus_received_amount (NEW)
        df['open_plus_received_amount'] = df['open_amount'] + df['received_amount']

        # Clean up temp column
        df.drop(columns=['_corrected_unit_cost'], inplace=True)

        # Log summary
        is_open_order = df['transaction_type'] == 'Open Orders'
        print(f"    Open Orders rows: {is_open_order.sum():,}")
        print(f"    Receipts rows: {is_receipt.sum():,}")
        print(f"    Total open_amount: ${df['open_amount'].sum():,.2f}")
        print(f"    Total received_amount: ${df['received_amount'].sum():,.2f}")
        print(f"    Total open_plus_received_amount: ${df['open_plus_received_amount'].sum():,.2f}")

        return df

    def _standardize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Consolidates all date and numeric conversions into a single pass.
        Cleans currency symbols and ensures correct data types for processing.
        """
        print("  Standardizing data types...")
        
        # 1. Clean currency symbols from potential numeric columns
        # Note: extended_amount comes from source CSV and is used for 1000x unit_cost detection
        # The new amount columns (open_amount, received_amount, etc.) are calculated later
        currency_cols = ['extended_amount', 'unit_cost', 'calculated_price']
        for col in currency_cols:
            # the currecny columns are float so it will not go into inside if statement
            if col in df.columns and df[col].dtype == 'object':
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r'[$,\s]', '', regex=True)
                    .replace(['', 'nan', 'None'], np.nan)
                )

        # 2. Bulk numeric conversion
        # Note: open_amount, received_amount, open_plus_received_* are calculated later
        numeric_cols = [
            'total_quantity', 'received_quantity', 'open_quantity', 'vendor_quantity',
            'unit_cost', 'extended_amount', 'calculated_price'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 3. Bulk date conversion
        date_cols = ['order_date', 'due_date', 'receipt_date', 'promise_date', 'old_promise_date', 'arrived_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 4. String types for ID columns
        if 'po_number' in df.columns:
            df['po_number'] = df['po_number'].astype(str)

        # 5. Convert spec columns to string (they contain mixed values like "10", "120/240V", "1P, 3P")
        spec_cols = [
            'item_spec_type', 'item_spec_amps', 'item_spec_voltage', 'item_spec_phase',
            'item_spec_poles', 'item_spec_kaic', 'item_spec_frame', 'item_spec_kva',
            'item_spec_gauge', 'item_spec_nema'
        ]
        #spec cols are not there in staging, it will come from master files later
        for col in spec_cols:
            if col in df.columns:
                # Convert to string, replacing NaN with empty string for clean Parquet export
                df[col] = df[col].fillna('').astype(str).replace('nan', '')

        return df

    def _add_calendar_attributes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add precomputed calendar attributes based on order_date.
        Moves this logic from Power Query into ETL.

        Adds:
        - order_year (int): Year of the order
        - order_month (int): Month of the order (1-12)
        - order_year_month (str): "YYYY-MM" format
        - order_quarter (str): "Q1", "Q2", "Q3", "Q4"
        """
        print("  Adding Calendar Attributes...")

        if 'order_date' not in df.columns:
            df['order_year'] = None
            df['order_month'] = None
            df['order_year_month'] = None
            df['order_quarter'] = None
            return df

        # Ensure order_date is datetime
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

        # Add calendar fields with proper nullable integer types
        df['order_year'] = df['order_date'].dt.year.astype('Int32')
        df['order_month'] = df['order_date'].dt.month.astype('Int32')
        df['order_year_month'] = df['order_date'].dt.strftime('%Y-%m')
        df['order_quarter'] = 'Q' + df['order_date'].dt.quarter.astype(str)

        # Handle nulls - replace 'Qnan' with None
        df.loc[df['order_date'].isna(), 'order_year_month'] = None
        df.loc[df['order_date'].isna(), 'order_quarter'] = None

        populated = df['order_year'].notna().sum()
        print(f"    Calendar attributes added for {populated:,} rows")

        return df

    def _apply_business_unit_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply business unit mapping from external CSV.
        Moves this logic from Power Query into ETL.

        Mapping:
        - EPICOR -> States
        - NETSUITE -> PwrQ
        - Other -> Other (fallback)
        """
        print("  Applying Business Unit Mapping...")
        row_count_before = len(df)

        if not os.path.exists(self.business_unit_map_path):
            print(f"    Warning: {self.business_unit_map_path} not found, using hardcoded defaults")
            df['business_unit'] = df['source_system'].map({
                'EPICOR': 'States',
                'NETSUITE': 'PwrQ'
            }).fillna('Other')
            return df

        # Load mapping table
        bu_map = pd.read_csv(self.business_unit_map_path)
        print(f"    Loaded {len(bu_map)} business unit mappings")

        # Merge on source_system
        df = df.merge(
            bu_map,
            on='source_system',
            how='left'
        )

        # Fill unmapped with 'Other'
        df['business_unit'] = df['business_unit'].fillna('Other')

        # Report
        for bu in df['business_unit'].unique():
            count = (df['business_unit'] == bu).sum()
            print(f"    {bu}: {count:,} rows")

        # Validate merge didn't create row explosion (many-to-many join)
        if len(df) != row_count_before:
            raise DataQualityError(
                f"Business unit merge changed row count: {row_count_before:,} -> {len(df):,}. "
                f"Check for duplicate source_system entries in business_unit_map.csv."
            )

        return df

    def _compute_analysis_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
          Compute analysis_date based on order status.
      
          Default Logic
          -------------
          Open Orders:
              due_date -> promise_date -> order_date
      
          Closed Orders:
              receipt_date -> due_date -> order_date
      
          NETSUITE_VANTRAN / SYTELINE
          ---------------------------
          Open Orders:
              order_date -> due_date -> promise_date
      
          Closed Orders:
              receipt_date -> due_date -> order_date
      
          No max_file_date adjustment is applied for
          NETSUITE_VANTRAN / SYTELINE.
    """

        print("  Computing Analysis Date...")

        # Initialize columns
        df['analysis_date'] = pd.NaT
        df['analysis_date_source'] = None
        df['analysis_date_is_fallback'] = False

        date_cols = [
            'due_date',
            'promise_date',
            'old_promise_date',
            'receipt_date',
            'order_date'
        ]

        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Handle is_open_order
        is_open = df['is_open_order']
        if is_open.dtype == 'object':
            is_open = is_open.astype(str).str.lower().isin(['true', '1', 'yes'])
        else:
            is_open = is_open.fillna(False).astype(bool)

        # ------------------------------------------------------------------
        # Source System Masks
        # ------------------------------------------------------------------

        special_sources = ['NETSUITE_VANTRAN', 'SYTELINE']

        special_mask = df['source_system'].isin(special_sources)

        special_open = special_mask & is_open
        special_closed = special_mask & ~is_open

        open_mask = ~special_mask & is_open
        closed_mask = ~special_mask & ~is_open

        # ==================================================================
        # SPECIAL SYSTEMS - OPEN
        # order_date -> due_date -> promise_date
        # ==================================================================

        mask = special_open & df['order_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'order_date']
        df.loc[mask, 'analysis_date_source'] = 'order_date'
        df.loc[mask, 'analysis_date_is_fallback'] = False

        mask = special_open & df['analysis_date'].isna() & df['due_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'due_date']
        df.loc[mask, 'analysis_date_source'] = 'due_date'

        mask = special_open & df['analysis_date'].isna() & df['promise_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'promise_date']
        df.loc[mask, 'analysis_date_source'] = 'promise_date'

        # ==================================================================
        # SPECIAL SYSTEMS - CLOSED
        # receipt_date -> due_date -> order_date
        # ==================================================================

        mask = special_closed & df['order_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'order_date']
        df.loc[mask, 'analysis_date_source'] = 'order_date'
        df.loc[mask, 'analysis_date_is_fallback'] = True

        mask = special_closed & df['due_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'due_date']
        df.loc[mask, 'analysis_date_source'] = 'due_date'
        df.loc[mask, 'analysis_date_is_fallback'] = False

        mask = special_closed & df['receipt_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'receipt_date']
        df.loc[mask, 'analysis_date_source'] = 'receipt_date'
        df.loc[mask, 'analysis_date_is_fallback'] = False

        # ==================================================================
        # EXISTING LOGIC - OPEN
        # due_date -> promise_date -> order_date
        # ==================================================================

        mask = open_mask & df['order_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'order_date']
        df.loc[mask, 'analysis_date_source'] = 'order_date'
        df.loc[mask, 'analysis_date_is_fallback'] = True

        mask = open_mask & df['promise_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'promise_date']
        df.loc[mask, 'analysis_date_source'] = 'promise_date'
        df.loc[mask, 'analysis_date_is_fallback'] = False

        mask = open_mask & df['due_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'due_date']
        df.loc[mask, 'analysis_date_source'] = 'due_date'
        df.loc[mask, 'analysis_date_is_fallback'] = False

        # ==================================================================
        # EXISTING LOGIC - CLOSED
        # receipt_date -> due_date -> order_date
        # ==================================================================

        mask = closed_mask & df['order_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'order_date']
        df.loc[mask, 'analysis_date_source'] = 'order_date'
        df.loc[mask, 'analysis_date_is_fallback'] = True

        mask = closed_mask & df['due_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'due_date']
        df.loc[mask, 'analysis_date_source'] = 'due_date'
        df.loc[mask, 'analysis_date_is_fallback'] = False

        mask = closed_mask & df['receipt_date'].notna()
        df.loc[mask, 'analysis_date'] = df.loc[mask, 'receipt_date']
        df.loc[mask, 'analysis_date_source'] = 'receipt_date'
        df.loc[mask, 'analysis_date_is_fallback'] = False

        # ==================================================================
        # Max File Date Adjustment
        # ONLY for normal open orders
        # ==================================================================

        max_file_date_str = self.pipeline_metadata.get('max_file_date')
    
        if max_file_date_str:
            max_file_date = pd.to_datetime(max_file_date_str)
    
            needs_adjustment = (
                open_mask &
                df['analysis_date'].notna() &
                (df['analysis_date'] < max_file_date)
            )
    
            adjustment_count = needs_adjustment.sum()
    
            if adjustment_count > 0:
                df.loc[needs_adjustment, 'analysis_date'] = max_file_date
                print(
                    f"    Adjusted {adjustment_count:,} OPEN_ORDER rows "
                    f"with past analysis_date to {max_file_date_str}"
                )

        # Report
        source_counts = df['analysis_date_source'].value_counts()
        print("    Analysis date sources:")
        for source, count in source_counts.items():
            print(f"      {source}: {count:,}")
        fallback_count = df['analysis_date_is_fallback'].sum()
        print(f"    Fallback to order_date: {fallback_count:,} rows")

        return df

    def _enforce_schema_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert columns to proper numeric types before Parquet export.
        This ensures Power BI doesn't need Table.TransformColumnTypes.

        Converts these text columns to numeric:
        - vendor_quantity, uninvoiced_qty_supplier, uninvoiced_qty_internal
        - rawmat_thickness, rawmat_width, rawmat_length
        - rawmat_weight_lbs, rawmat_total_weight_lbs, rawmat_price_per_lb
        """
        print("  Enforcing Schema Types...")

        converted_count = 0
        for col in CONVERTED_TO_NUMERIC:
            if col in df.columns:
                # Check if column is currently non-numeric
                if df[col].dtype == 'object':
                    before_nulls = df[col].isna().sum()
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    after_nulls = df[col].isna().sum()
                    coerced = after_nulls - before_nulls
                    if coerced > 0:
                        print(f"    {col}: converted to numeric ({coerced} values coerced to null)")
                    converted_count += 1

        print(f"    Converted {converted_count} columns to numeric types")

        return df

    def _run_data_quality_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run data quality checks and generate report.
        Fails pipeline if critical issues found.
        """
        print("\n  Running Data Quality Checks...")

        checker = DataQualityChecker(df)
        results = checker.run_all_checks()

        # Print summary
        print(checker.generate_report())

        # Save report to file
        dq_report_path = os.path.join(self.output_dir, "data_quality_report.txt")
        checker.save_report(dq_report_path)
        print(f"\n  DQ report saved to: {dq_report_path}")

        # Fail on critical issues
        if not checker.passed:
            critical_failures = checker.get_critical_failures()
            failure_messages = [f"{r.check_name}: {r.message}" for r in critical_failures]
            raise DataQualityError(
                f"Pipeline failed due to critical data quality issues:\n" +
                "\n".join(failure_messages)
            )

        return df

    def _save_parquet_with_schema(self, df: pd.DataFrame, output_path: str):
        """
        Save Parquet with explicit pyarrow schema and metadata.
        This ensures Power BI gets correct types without transformations.
        """
        print(f"  Saving Parquet with explicit schema...")

        # Add metadata columns
        df['_schema_version'] = SCHEMA_VERSION
        df['_load_timestamp'] = pd.Timestamp.now()
        df['_data_as_of_date'] = pd.to_datetime(self.pipeline_metadata.get('max_file_date'))

        # Convert date fields to date (not datetime) for Parquet
        if 'analysis_date' in df.columns:
            df['analysis_date'] = pd.to_datetime(df['analysis_date'], errors='coerce').dt.date
        if 'final_receipt_date' in df.columns:
            df['final_receipt_date'] = pd.to_datetime(df['final_receipt_date'], errors='coerce').dt.date

        # Get the schema
        schema = get_po_fact_schema()
        metadata = get_schema_metadata()

        # Filter schema to only columns that exist in dataframe
        existing_cols = set(df.columns)
        schema_fields = [(f.name, f.type) for f in schema if f.name in existing_cols]

        # Add any columns in df but not in schema (with inferred types)
        schema_col_names = {f.name for f in schema}
        extra_cols = existing_cols - schema_col_names
        if extra_cols:
            print(f"    Note: {len(extra_cols)} columns not in schema will use inferred types")

        # Coerce string-typed schema columns that may arrive as int
        for col in ('po_line', 'vendor_id'):
            if col in df.columns:
                df[col] = df[col].astype(str)

        try:
            # Try to use explicit schema for matching columns
            filtered_schema = pa.schema(schema_fields)
            table = pa.Table.from_pandas(df, schema=filtered_schema, preserve_index=False)
        except Exception as e:
            print(f"    Schema enforcement warning: {e}")
            print("    Falling back to inferred schema")
            table = pa.Table.from_pandas(df, preserve_index=False)

        # Add custom metadata
        existing_metadata = table.schema.metadata or {}
        merged_metadata = {
            **existing_metadata,
            **{k.encode(): v.encode() for k, v in metadata.items()}
        }
        table = table.replace_schema_metadata(merged_metadata)

        # Write with compression
        pq.write_table(table, output_path, compression='snappy')

        print(f"    Schema version: {SCHEMA_VERSION}")
        print(f"    Columns: {len(df.columns)}")

    def run(self):
        """Main execution method."""
        print("\nStarting Final Assembly...")
        print(f"  Schema Version: {SCHEMA_VERSION}")

        # Load staging data
        if not os.path.exists(self.staging_path):
            print(f"  Staging file not found: {self.staging_path}")
            return

        # Keep literal placeholders like 'N/A' as strings instead of auto-converting to NaN.
        df = pd.read_csv(self.staging_path, low_memory=False, keep_default_na=False)
        print(f"  Loaded {len(df):,} staging rows.")

        # ============================================
        # PHASE 1: Base Processing (existing)
        # ============================================

        # Standardize data types early
        df = self._standardize_data_types(df)

        # Apply vendor standardization
        print("  Applying Vendor Standardization...")
        df = self._apply_vendor_standardization(df)

        # Apply terms standardization
        print("  Applying Terms Standardization...")
        df = self._apply_terms_standardization(df)

        print("  Applying Item Categorization...")
        df = self._apply_item_categorization(df)

        # Standardize item_id to MPN (item_name_mpn is consistent across systems;
        # raw item_id is a meaningless numeric ID in NetSuite)
        print("  Standardizing Item IDs...")
        if 'item_name_mpn' in df.columns:
            valid_mpn = df['item_name_mpn'].notna() & (df['item_name_mpn'].astype(str).str.strip() != '')
            standardized = valid_mpn.sum()
            df.loc[valid_mpn, 'item_id'] = df.loc[valid_mpn, 'item_name_mpn']
            print(f"    Standardized {standardized:,} item_id values to MPN")

        df = self._resolve_category_conflicts(df)

        # Apply description fallback (replace placeholders with display name)
        print("  Applying Description Fallback...")
        df = self._apply_description_fallback(df)

        # Build unified item_name (requires all description sources populated)
        print("  Building Unified Item Name...")
        df = self._build_item_name(df)

        # Create shortened item name for PowerBI visuals (max 25 chars)
        # Derived from item_name (descriptive) instead of item_name_mpn (part number)
        print("  Creating Shortened Item Names...")
        df['item_short_name'] = df['item_name'].apply(create_short_name)
        df = self._fill_blank_item_labels_for_reporting(df)
        short_count = (df['item_short_name'].str.len() < df['item_name'].astype(str).str.len()).sum()
        print(f"    Shortened {short_count:,} item names")

        # ============================================
        # PHASE 2: New ETL Enhancements
        # ============================================

        # Add calendar attributes (replaces Power Query calculation)
        df = self._add_calendar_attributes(df)

        # Apply business unit mapping (replaces Power Query calculation)
        df = self._apply_business_unit_mapping(df)

        # Compute analysis_date (replaces Power Query calculation)
        df = self._compute_analysis_date(df)

        # Calculate OTD at PO-line grain BEFORE reporting-date filtering.
        # This is required so historical receipt rows are still available to
        # determine the final receipt date for a partially delivered PO line.
        print("  Calculating PO-line OTD metrics...")
        df = self._add_status_flags(df)
        df = self._add_otd_metrics(df)

        # Apply reporting date filter after OTD line-level evaluation.
        df = self._apply_reporting_date_filter(df)

        # ============================================
        # PHASE 3: Metrics Calculation (existing)
        # ============================================

        # Calculate raw material metrics (weight, price per lb)
        print("  Calculating Raw Material Metrics...")
        df = self._calculate_rawmat_metrics(df)

        # Apply copper price index for copper products
        df = self._apply_copper_price_index(df)

        # Add calculated metrics
        print("  Adding Calculated Metrics...")
        df = self._add_calculated_metrics(df)

        # ============================================
        # PHASE 4: Schema Enforcement & Validation
        # ============================================

        # Enforce schema types (convert text to numeric)
        df = self._enforce_schema_types(df)

        # Make blank item_id values visible in Power BI slicers/tables
        df = self._fill_blank_item_ids_for_reporting(df)

        # Make blank po_number values visible in Power BI slicers/tables
        df = self._fill_blank_po_numbers_for_reporting(df)

        # Run data quality checks (fails on critical issues)
        df = self._run_data_quality_checks(df)

        # ============================================
        # PHASE 5: Output
        # ============================================

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Save CSV (for backwards compatibility)
        csv_path = os.path.join(self.output_dir, "po_fact_table.csv")
        df.to_csv(csv_path, index=False)
        print(f"  Saved PO Fact Table (CSV) to: {csv_path}")

        # Save Parquet with explicit schema
        parquet_path = os.path.join(self.output_dir, "po_fact_table.parquet")
        try:
            # Convert spec columns to string to avoid mixed-type errors
            spec_cols = [
                'item_spec_type', 'item_spec_amps', 'item_spec_voltage', 'item_spec_phase',
                'item_spec_poles', 'item_spec_kaic', 'item_spec_frame', 'item_spec_kva',
                'item_spec_gauge', 'item_spec_nema'
            ]
            for col in spec_cols:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str).replace('nan', '')

            # Use new schema-aware save method
            self._save_parquet_with_schema(df, parquet_path)
            print(f"  Saved PO Fact Table (Parquet) to: {parquet_path}")

        except ImportError:
            print("  Warning: 'pyarrow' not installed. Skipping Parquet export.")
        except Exception as e:
            print(f"  Error saving Parquet: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n  === FINAL OUTPUT ===")
        print(f"  Total rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")

        # Print metric summary
        self._print_metrics_summary(df)

    def _print_metrics_summary(self, df: pd.DataFrame):
        """Prints a summary of calculated metrics."""
        print("\n  --- Metrics Summary ---")

        if 'is_overdue' in df.columns:
            overdue_count = df['is_overdue'].sum()
            print(f"  Overdue POs: {overdue_count:,}")

        if 'is_fully_received' in df.columns:
            fully_received = df['is_fully_received'].sum()
            print(f"  Fully Received: {fully_received:,}")

        if 'on_time_flag' in df.columns:
            on_time = df['on_time_flag'].dropna()
            if len(on_time) > 0:
                on_time_count = (on_time == 'On Time').sum()
                late_count = (on_time == 'Late').sum()
                total = len(on_time)
                otd_pct = (on_time_count / total) * 100
                print(f"  OTD Rate: {otd_pct:.1f}% — On Time={on_time_count:,}, Late={late_count:,}")

        # 6-Column Quantity/Amount Summary
        if all(col in df.columns for col in ['open_amount', 'received_amount', 'open_plus_received_amount']):
            print(f"\n  --- 6-Column Quantity/Amount Summary ---")
            print(f"  Total Open Amount:              ${df['open_amount'].sum():,.2f}")
            print(f"  Total Received Amount:          ${df['received_amount'].sum():,.2f}")
            print(f"  Total Open+Received Amount:     ${df['open_plus_received_amount'].sum():,.2f}")

            # Breakdown by transaction type
            open_orders = df[df['transaction_type'] == 'Open Orders']
            receipts = df[df['transaction_type'] == 'Receipts']

            if len(open_orders) > 0:
                print(f"\n  OPEN_ORDER ({len(open_orders):,} rows):")
                print(f"    Open Amount:              ${open_orders['open_amount'].sum():,.2f}")
                print(f"    Received Amount:          ${open_orders['received_amount'].sum():,.2f}")
                print(f"    Open+Received Amount:     ${open_orders['open_plus_received_amount'].sum():,.2f}")

            if len(receipts) > 0:
                print(f"\n  RECEIPT ({len(receipts):,} rows):")
                print(f"    Open Amount (should be 0): ${receipts['open_amount'].sum():,.2f}")
                print(f"    Received Amount:           ${receipts['received_amount'].sum():,.2f}")

        if 'spend_bucket' in df.columns:
            print(f"\n  Spend Distribution:")
            for bucket, count in df['spend_bucket'].value_counts().items():
                print(f"    {bucket}: {count:,}")


if __name__ == "__main__":
    builder = FinalBuilder(
        staging_path="step_2_enrichment/b.staging/pre_cleaned_staging.csv",
        vendor_map_path="step_3_maintenance/a.mappings/vendor_map.csv",
        item_map_path="step_3_maintenance/a.mappings/item_map.csv",
        output_dir="step_4_assembly/b.final"
    )
    builder.run()
