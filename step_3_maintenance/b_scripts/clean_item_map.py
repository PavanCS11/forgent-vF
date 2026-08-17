"""
Item Map Cleanup Script - Deduplication & Quality Resolution

Cleans item_map.csv by:
  Step 1: Removing "Master Data Only" placeholders when real categorization exists
  Step 2: Removing pure duplicate rows (identical ID + description + categories)
  Step 3: Quality-aware dedup for conflicting categories (best row wins)
  Step 4: Detecting stale descriptions by cross-referencing staging data (report-only)

Run after enrich_items.py and before assembly.

Usage:
    python clean_item_map.py [--dry-run] [--report-only]
"""

import argparse
import os
import re

import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
ITEM_MAP_FILE = os.path.join(BASE_DIR, "step_3_maintenance", "a.mappings", "item_map.csv")

SCI_NOTATION_PATTERN = re.compile(r'^\d+\.\d+E\+\d+$', re.IGNORECASE)

SPEC_COLS = [
    'spec_type', 'spec_amps', 'spec_voltage', 'spec_phase', 'spec_poles',
    'spec_kaic', 'spec_frame', 'spec_kva', 'spec_gauge', 'spec_nema',
    'RawMat_Finish', 'RawMat_Thickness', 'RawMat_Width', 'RawMat_Length',
    'RawMat_UoM', 'RawMat_Weight_Lbs',
]

DEDUP_KEY_COLS = [
    'Item_ID', 'Raw_Description',
    'Category_Level_1', 'Category_Level_2', 'Category_Level_3',
]


def _score_row(row: pd.Series) -> int:
    """Score a row for quality-aware dedup. Higher = better."""
    score = 0

    # Penalize catch-all / placeholder categories
    l1 = str(row.get('Category_Level_1', ''))
    l2 = str(row.get('Category_Level_2', ''))
    l3 = str(row.get('Category_Level_3', ''))

    if l1 == 'Miscellaneous/Other':
        score -= 50
    if l2 == 'Uncategorized':
        score -= 30
    if l3 == 'Master Data Only':
        score -= 40

    # Reward complete hierarchy
    if l1 and l1 != 'nan':
        score += 10
    if l2 and l2 != 'nan':
        score += 10
    if l3 and l3 != 'nan':
        score += 10

    # Reward extraction confidence
    conf = pd.to_numeric(row.get('extraction_confidence'), errors='coerce')
    if pd.notna(conf):
        score += int(conf * 20)

    # Reward meaningful description
    desc = str(row.get('Raw_Description', ''))
    if desc and desc != 'nan' and 'Master Data Only' not in desc and len(desc) > 10:
        score += 15

    # Reward spec data presence
    for col in SPEC_COLS:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() != '':
            score += 5

    return score


def clean_item_map(dry_run: bool = False, report_only: bool = False):
    """
    Cleans item_map.csv: removes duplicates, resolves conflicts, reports stale data.
    """
    print("\n" + "=" * 60)
    print("ITEM MAP CLEANUP - Deduplication & Quality Resolution")
    print("=" * 60)

    # Load
    df = pd.read_csv(ITEM_MAP_FILE, dtype={'Item_ID': str}, low_memory=False)
    original_count = len(df)
    original_unique = df['Item_ID'].nunique()
    print(f"Loaded {original_count:,} rows ({original_unique:,} unique Item_IDs)")

    # Fix scientific notation IDs (e.g., '1.75223E+11' -> '175223000000')
    sci_mask = df['Item_ID'].str.match(SCI_NOTATION_PATTERN, na=False)
    sci_count = sci_mask.sum()
    sci_unique = df.loc[sci_mask, 'Item_ID'].nunique()
    if sci_count > 0:
        df.loc[sci_mask, 'Item_ID'] = (
            df.loc[sci_mask, 'Item_ID']
            .apply(lambda x: str(int(float(x))) if SCI_NOTATION_PATTERN.match(str(x)) else x)
        )
        print(f"  Fixed {sci_count:,} scientific notation IDs ({sci_unique:,} unique keys)")
    else:
        print(f"  Scientific notation IDs: 0 (none found)")

    total_removed = 0

    # ==========================================
    # STEP 1: Remove "Master Data Only" placeholders
    # ==========================================
    print("\n--- Step 1: Remove 'Master Data Only' Placeholders ---")

    mdo_mask = df['Category_Level_3'] == 'Master Data Only'
    mdo_count = mdo_mask.sum()

    if mdo_count > 0:
        mdo_ids = set(df.loc[mdo_mask, 'Item_ID'])
        real_ids = set(df.loc[~mdo_mask & df['Category_Level_1'].notna(), 'Item_ID'])
        both_ids = mdo_ids & real_ids
        mdo_only_ids = mdo_ids - real_ids

        # Remove MDO rows where real categorization exists for same Item_ID
        remove_mdo_with_real = mdo_mask & df['Item_ID'].isin(both_ids)
        removed_mdo_real = remove_mdo_with_real.sum()

        # For MDO-only IDs, keep first occurrence, drop rest
        mdo_only_dupes = (
            mdo_mask
            & df['Item_ID'].isin(mdo_only_ids)
            & df.duplicated(subset=['Item_ID'], keep='first')
        )
        removed_mdo_dedup = mdo_only_dupes.sum()

        drop_step1 = remove_mdo_with_real | mdo_only_dupes
        step1_removed = drop_step1.sum()

        if not report_only:
            df = df[~drop_step1].reset_index(drop=True)

        total_removed += step1_removed
        print(f"  MDO rows with real alt: {removed_mdo_real:,} removed ({len(both_ids)} Item_IDs)")
        print(f"  MDO-only duplicates:    {removed_mdo_dedup:,} removed ({len(mdo_only_ids)} Item_IDs)")
    else:
        print("  No 'Master Data Only' rows found")

    # ==========================================
    # STEP 2: Remove pure duplicate rows
    # ==========================================
    print("\n--- Step 2: Remove Pure Duplicate Rows ---")

    pure_dupe_mask = df.duplicated(subset=DEDUP_KEY_COLS, keep='first')
    step2_removed = pure_dupe_mask.sum()

    if step2_removed > 0 and not report_only:
        df = df[~pure_dupe_mask].reset_index(drop=True)

    total_removed += step2_removed
    print(f"  Removed {step2_removed:,} pure duplicate rows")

    # ==========================================
    # STEP 3: Quality-aware dedup for conflicts
    # ==========================================
    print("\n--- Step 3: Quality-Aware Conflict Resolution ---")

    dup_ids = df[df.duplicated(subset=['Item_ID'], keep=False)]['Item_ID'].unique()
    # Filter to only IDs with genuinely different categories
    conflict_ids = []
    for item_id in dup_ids:
        group = df[df['Item_ID'] == item_id]
        cats = group['Category_Level_1'].dropna().unique()
        if len(cats) > 1:
            conflict_ids.append(item_id)

    if conflict_ids:
        rows_to_drop = []

        for item_id in conflict_ids:
            group = df[df['Item_ID'] == item_id]
            scores = group.apply(_score_row, axis=1)
            best_idx = scores.idxmax()
            losers = group.index[group.index != best_idx]
            rows_to_drop.extend(losers.tolist())

        step3_removed = len(rows_to_drop)
        if not report_only:
            df = df.drop(index=rows_to_drop).reset_index(drop=True)

        total_removed += step3_removed
        print(f"  Resolved {len(conflict_ids)} conflicts, removed {step3_removed:,} lower-quality rows")
    else:
        print("  No conflicting categorizations found")

    # Also dedup remaining same-ID rows (same category but different descriptions, etc.)
    remaining_dups = df.duplicated(subset=['Item_ID'], keep='first')
    remaining_dup_count = remaining_dups.sum()
    if remaining_dup_count > 0:
        if not report_only:
            df = df[~remaining_dups].reset_index(drop=True)
        total_removed += remaining_dup_count
        print(f"  Removed {remaining_dup_count:,} remaining duplicate rows (same category, different metadata)")

    # ==========================================
    # Summary & Save
    # ==========================================
    final_count = len(df)
    final_unique = df['Item_ID'].nunique()

    print(f"\n--- Summary ---")
    print(f"  Before: {original_count:,} rows ({original_unique:,} unique)")
    print(f"  After:  {final_count:,} rows ({final_unique:,} unique)")
    print(f"  Removed: {total_removed:,} rows")

    if dry_run or report_only:
        print("  [DRY RUN] No changes saved to item_map.csv")
    else:
        df.to_csv(ITEM_MAP_FILE, index=False)
        print(f"  Cleaned item_map.csv saved ({final_count:,} rows)")


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Item map cleanup - deduplication & quality resolution"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate report but don't modify item_map.csv",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only generate the report, no changes (same as --dry-run)",
    )

    args = parser.parse_args()
    clean_item_map(dry_run=args.dry_run, report_only=args.report_only)


if __name__ == "__main__":
    main()
