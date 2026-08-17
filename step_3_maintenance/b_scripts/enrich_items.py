"""
Item Enrichment Script - Waterfall Categorization

This script auto-categorizes uncategorized items using a waterfall approach:
Step 1: Description Match - If description matches an existing categorized item, inherit categories
Step 2: AI Categorization - Use AI to categorize remaining uncategorized items (optional)
Step 3: Electrical Spec Extraction - Extract specs for electrical items (optional)
Step 4: Raw Material Spec Extraction - Extract specs for raw materials (optional)

Run after refresh_mappings.py to automatically categorize new items.

Usage:
    python enrich_items.py [--skip-ai] [--dry-run]
"""

import argparse
import pandas as pd
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
ITEM_MAP_FILE = os.path.join(BASE_DIR, "step_3_maintenance", "a.mappings", "item_map.csv")


def enrich_items(skip_ai: bool = False, dry_run: bool = False):
    """
    Enriches uncategorized items using a waterfall approach.
    Step 1: Match descriptions to existing categorized items.
    Step 2-4: AI enrichment (if not skipped).
    """
    print("\n" + "="*60)
    print("ITEM ENRICHMENT - Waterfall Categorization")
    print("="*60)

    # Load item map
    df = pd.read_csv(ITEM_MAP_FILE, low_memory=False)
    print(f"Loaded {len(df):,} items from item_map.csv")

    # Identify uncategorized items (no Category_Level_1, 2, or 3)
    uncategorized_mask = (
        df['Category_Level_1'].isna() &
        df['Category_Level_2'].isna() &
        df['Category_Level_3'].isna()
    )
    uncategorized_count = uncategorized_mask.sum()
    print(f"Uncategorized items: {uncategorized_count:,}")

    if uncategorized_count == 0:
        print("No uncategorized items to process.")
        return

    # ==========================================
    # STEP 1: Description Match
    # ==========================================
    print("\n--- Step 1: Description Match ---")

    # Build lookup from categorized items: description -> category info
    categorized = df[~uncategorized_mask].copy()
    categorized['desc_key'] = categorized['Raw_Description'].str.lower().str.strip()

    # Get first occurrence of each description (to handle multiple items with same desc)
    desc_to_category = {}
    for _, row in categorized.iterrows():
        desc_key = row['desc_key']
        if desc_key and desc_key not in desc_to_category and pd.notna(row['Category_Level_1']):
            desc_to_category[desc_key] = {
                'Category': row.get('Category', ''),
                'Sub_Category': row.get('Sub_Category', ''),
                'Category_Level_1': row['Category_Level_1'],
                'Category_Level_2': row.get('Category_Level_2', ''),
                'Category_Level_3': row.get('Category_Level_3', '')
            }

    print(f"Built lookup with {len(desc_to_category):,} unique categorized descriptions")

    # Match uncategorized items
    step1_matched = 0
    for idx in df[uncategorized_mask].index:
        desc = str(df.at[idx, 'Raw_Description']).lower().strip()
        if desc in desc_to_category:
            cat_info = desc_to_category[desc]
            df.at[idx, 'Category'] = cat_info['Category']
            df.at[idx, 'Sub_Category'] = cat_info['Sub_Category']
            df.at[idx, 'Category_Level_1'] = cat_info['Category_Level_1']
            df.at[idx, 'Category_Level_2'] = cat_info['Category_Level_2']
            df.at[idx, 'Category_Level_3'] = cat_info['Category_Level_3']
            step1_matched += 1

    print(f"Step 1 matched: {step1_matched:,} items")

    # ==========================================
    # Step 2-4: AI Enrichment (optional)
    # ==========================================
    # Save description matches before AI enrichment (or as final save if skipping AI)
    if not dry_run:
        df.to_csv(ITEM_MAP_FILE, index=False)
        print("Saved description matches to item_map.csv")
    else:
        print("[DRY RUN] No changes saved")

    if not skip_ai:
        print("\n--- Steps 2-4: AI Enrichment ---")
        try:
            from step_3_maintenance.c_ai_enrichment.run_ai_enrichment import run_ai_enrichment

            # run_ai_enrichment loads, enriches, and saves the file independently
            run_ai_enrichment(dry_run=dry_run)

        except ImportError as e:
            print(f"  AI enrichment module not available: {e}")
            print("  Skipping AI enrichment steps")
        except Exception as e:
            print(f"  AI enrichment failed: {e}")
            print("  Description matches were saved; AI enrichment portion failed")
            raise  # Let pipeline_runner decide severity
    else:
        print("\n--- Skipping AI Enrichment (--skip-ai flag) ---")

    # Reload for final summary (AI enrichment may have updated the file)
    df = pd.read_csv(ITEM_MAP_FILE, low_memory=False)

    remaining_uncategorized = (df['Category_Level_1'] == 'Uncategorized').sum()
    remaining_empty = (
        df['Category_Level_1'].isna() &
        df['Category_Level_2'].isna() &
        df['Category_Level_3'].isna()
    ).sum()

    print(f"\n--- Final Summary ---")
    print(f"Step 1 (Description Match): {step1_matched:,} items")
    print(f"Remaining 'Uncategorized': {remaining_uncategorized:,}")
    print(f"Remaining empty categories: {remaining_empty:,}")


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Item enrichment - waterfall categorization"
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip AI enrichment steps (2-4)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save changes, just preview",
    )

    args = parser.parse_args()
    enrich_items(skip_ai=args.skip_ai, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
