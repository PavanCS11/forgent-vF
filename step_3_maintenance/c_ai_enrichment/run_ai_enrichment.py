"""
AI Enrichment Runner
====================

Standalone script to run AI-powered categorization and spec extraction.
Can be run independently from the main pipeline.

Usage:
    python run_ai_enrichment.py [options]

Options:
    --dry-run              Don't save changes, just preview
    --skip-categorization  Skip AI categorization step
    --skip-electrical      Skip electrical spec extraction
    --skip-raw-materials   Skip raw material spec extraction
    --limit N              Process only first N uncategorized items
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

import pandas as pd

# Add parent directories to path for imports
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
sys.path.insert(0, str(SCRIPT_DIR))

from ai_categorizer import AICategorizer
from spec_extractor import SpecExtractor
from taxonomy import (
    ELECTRICAL_CATEGORIES,
    RAW_MATERIAL_CATEGORY,
    CONFIDENCE_TEXT_TO_NUMERIC,
    VALID_ANALYSIS_TYPES,
)

# Path configuration
SCRIPT_DIR = Path(__file__).parent
MAPPINGS_DIR = SCRIPT_DIR.parent / "a.mappings"
ITEM_MAP_PATH = MAPPINGS_DIR / "item_map.csv"
FAILED_ITEMS_LOG = SCRIPT_DIR / "failed_items.log"
SPEC_CACHE_PATH = SCRIPT_DIR / "spec_cache.json"

# Descriptions that should be skipped (no useful specs extractable)
SKIP_DESCRIPTION_PATTERNS = [
    re.compile(r"^.{0,4}$"),  # Too short (< 5 chars)
    re.compile(r"^\d+(?:\.\d+)?[eE]\+?\d+$"),  # Scientific notation (Excel corruption)
    re.compile(r"^(?:lot|misc|freight|shipping|tax|handling|service|labor|charge)\b", re.IGNORECASE),
    re.compile(r"^(?:order by description|see attached|per quote|as per|call for pricing)\b", re.IGNORECASE),
    re.compile(r"^(?:credit|return|refund|adjustment|discount)\b", re.IGNORECASE),
    re.compile(r"^(?:temp |temporary )", re.IGNORECASE),
]


def build_item_index(df) -> dict:
    """Build reverse index for O(1) Item_ID -> row index lookups."""
    return {str(item_id): idx for idx, item_id in enumerate(df["Item_ID"])}


def safe_numeric(value, column_name: str):
    """
    Safely convert value to numeric for DataFrame assignment.
    Handles string values like '150 VA' by extracting the number.
    """
    if value is None or pd.isna(value):
        return None

    # Already numeric
    if isinstance(value, (int, float)):
        return value

    # Try to extract number from string (e.g., "150 VA" -> 150)
    if isinstance(value, str):
        import re
        # Extract first number from string
        match = re.search(r"(\d+(?:\.\d+)?)", value)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        # If no number found, return None for numeric columns
        return None

    return value


def clean_data_quality(df) -> tuple:
    """
    Clean and standardize data quality issues.

    Returns:
        tuple: (df, stats_dict) - cleaned DataFrame and statistics
    """
    stats = {"confidence_fixed": 0, "analysis_type_fixed": 0}

    # 1. Standardize confidence scores: convert text labels to numeric
    if "extraction_confidence" in df.columns:
        for text_val, numeric_val in CONFIDENCE_TEXT_TO_NUMERIC.items():
            mask = df["extraction_confidence"] == text_val
            count = mask.sum()
            if count > 0:
                df.loc[mask, "extraction_confidence"] = numeric_val
                stats["confidence_fixed"] += count

        # Convert to numeric, coercing errors to NaN
        df["extraction_confidence"] = pd.to_numeric(
            df["extraction_confidence"], errors="coerce"
        )

    # 2. Fix invalid analysis_type values (move product types to spec_type)
    if "analysis_type" in df.columns:
        invalid_mask = (
            df["analysis_type"].notna()
            & ~df["analysis_type"].isin(VALID_ANALYSIS_TYPES + [""])
        )
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            # These are likely product types like "Circuit Breaker", "Raw Material"
            # Move to spec_type if spec_type is empty
            for idx in df[invalid_mask].index:
                old_value = df.at[idx, "analysis_type"]
                if pd.isna(df.at[idx, "spec_type"]) or df.at[idx, "spec_type"] == "":
                    df.at[idx, "spec_type"] = old_value
                df.at[idx, "analysis_type"] = ""
            stats["analysis_type_fixed"] = invalid_count

    return df, stats


def load_failed_items() -> dict:
    """Load failed items from log file, grouped by step."""
    failed = {"categorization": set(), "electrical_extraction": set(), "raw_material_extraction": set()}
    if FAILED_ITEMS_LOG.exists():
        with open(FAILED_ITEMS_LOG) as f:
            for line in f:
                line = line.strip()
                if "," in line:
                    item_id, step = line.split(",", 1)
                    if step in failed:
                        failed[step].add(item_id)
    return failed


MAX_SPEC_CACHE_ENTRIES = 50000


def _evict_cache(cache: dict, max_entries: int) -> dict:
    """Evict oldest entries if cache exceeds max size. Dict preserves insertion order (Python 3.7+)."""
    if len(cache) <= max_entries:
        return cache
    evicted = len(cache) - max_entries
    keys = list(cache.keys())
    for key in keys[:evicted]:
        del cache[key]
    print(f"  Cache eviction: removed {evicted:,} oldest entries, kept {len(cache):,}")
    return cache


def load_spec_cache() -> dict:
    """Load the spec extraction cache from disk."""
    if SPEC_CACHE_PATH.exists():
        try:
            with open(SPEC_CACHE_PATH, "r") as f:
                cache = json.load(f)
            return _evict_cache(cache, MAX_SPEC_CACHE_ENTRIES)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_spec_cache(cache: dict):
    """Save the spec extraction cache to disk."""
    try:
        with open(SPEC_CACHE_PATH, "w") as f:
            json.dump(cache, f)
    except IOError as e:
        print(f"  Warning: Could not save spec cache: {e}")


def desc_cache_key(description: str) -> str:
    """Generate a cache key from a description."""
    normalized = description.strip().lower()
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def should_skip_description(description: str) -> bool:
    """Check if a description is obviously invalid for spec extraction."""
    if not description or not isinstance(description, str):
        return True
    desc = description.strip()
    for pattern in SKIP_DESCRIPTION_PATTERNS:
        if pattern.search(desc):
            return True
    return False


def filter_invalid_items(items: list) -> tuple:
    """
    Separate valid items from obviously invalid ones.

    Returns:
        tuple: (valid_items, skipped_count)
    """
    valid = []
    skipped = 0
    for item in items:
        desc = str(item.get("Raw_Description", "")).strip()
        if should_skip_description(desc):
            skipped += 1
        else:
            valid.append(item)
    return valid, skipped


def run_ai_enrichment(
    dry_run: bool = False,
    skip_categorization: bool = False,
    skip_electrical: bool = False,
    skip_raw_materials: bool = False,
    limit: int = None,
    reprocess_failed: bool = False,
):
    """
    Run AI enrichment on item_map.csv.

    Args:
        dry_run: If True, don't save changes.
        skip_categorization: Skip the AI categorization step.
        skip_electrical: Skip electrical spec extraction.
        skip_raw_materials: Skip raw material spec extraction.
        limit: Process only first N uncategorized items (for testing).
        reprocess_failed: If True, only reprocess items from failed_items.log.
    """
    # Load failed items if reprocessing
    failed_item_sets = None
    if reprocess_failed:
        failed_item_sets = load_failed_items()
        total_failed = sum(len(v) for v in failed_item_sets.values())
        if total_failed == 0:
            print("No failed items to reprocess.")
            return
        print(f"[REPROCESS MODE] Found {total_failed} failed items to reprocess")
    print("\n" + "=" * 60)
    print("AI ENRICHMENT - Categorization & Spec Extraction")
    print("=" * 60)

    if dry_run:
        print("[DRY RUN MODE - No changes will be saved]")

    # Load item map
    print(f"\nLoading item map from: {ITEM_MAP_PATH}")
    df = pd.read_csv(ITEM_MAP_PATH, low_memory=False)
    print(f"Total items: {len(df):,}")

    # Build index for O(1) lookups
    item_index = build_item_index(df)
    print(f"Built item index ({len(item_index):,} items)")

    # Clean data quality issues
    print("\nCleaning data quality issues...")
    df, clean_stats = clean_data_quality(df)
    if clean_stats["confidence_fixed"] > 0:
        print(f"  Standardized {clean_stats['confidence_fixed']:,} confidence values")
    if clean_stats["analysis_type_fixed"] > 0:
        print(f"  Fixed {clean_stats['analysis_type_fixed']:,} invalid analysis_type values")

    # Track failed items for logging
    failed_items = []

    # Load spec cache
    spec_cache = load_spec_cache()
    if spec_cache:
        print(f"Loaded spec cache ({len(spec_cache):,} entries)")

    # Initialize components
    try:
        categorizer = AICategorizer()
        extractor = SpecExtractor(cache=spec_cache)
        print("API connection initialized")
    except ValueError as e:
        print(f"ERROR: {e}")
        print("Please ensure OPENROUTER_API_KEY is set in .env file")
        return

    # =========================================================================
    # STEP 1: AI Categorization
    # =========================================================================
    if not skip_categorization:
        print("\n" + "-" * 40)
        print("Step 1: AI Categorization")
        print("-" * 40)

        # Find uncategorized items
        # uncategorized_mask = df["Category_Level_1"] == "Uncategorized"
        uncategorized_mask = (df["Category_Level_1"].isna() | (df["Category_Level_1"] == "") |(df["Category_Level_1"] == "Uncategorized"))
        uncategorized = df[uncategorized_mask].copy()

        print(f"Found {len(uncategorized):,} uncategorized items")

        if len(uncategorized) > 0:
            if limit:
                uncategorized = uncategorized.head(limit)
                print(f"Processing first {limit} items (--limit flag)")

            # Process in chunks to save progress incrementally
            CHUNK_SIZE = 1000
            total_items = len(uncategorized)
            total_categorized = 0

            for chunk_start in range(0, total_items, CHUNK_SIZE):
                chunk_end = min(chunk_start + CHUNK_SIZE, total_items)
                chunk = uncategorized.iloc[chunk_start:chunk_end]

                print(f"\n  Processing items {chunk_start+1:,}-{chunk_end:,} of {total_items:,}...")

                items = chunk[["Item_ID", "Raw_Description"]].to_dict("records")
                results = categorizer.categorize_all(items)

                # Apply results to dataframe using O(1) index lookup
                categorized_count = 0
                for result in results:
                    item_id = str(result.get("Item_ID", ""))
                    idx = item_index.get(item_id)
                    if idx is not None and result.get("Category_Level_1") != "Uncategorized":
                        for col in [
                            "Category_Level_1",
                            "Category_Level_2",
                            "Category_Level_3",
                            "extraction_confidence",
                            "analysis_type",
                        ]:
                            if col in result:
                                df.at[idx, col] = result[col]
                        categorized_count += 1
                    elif idx is not None and result.get("Category_Level_1") == "Uncategorized":
                        failed_items.append({"Item_ID": item_id, "step": "categorization"})

                total_categorized += categorized_count
                print(f"  Chunk complete: {categorized_count:,} categorized, {total_categorized:,} total so far")

                # Save progress incrementally (every chunk)
                if not dry_run and categorized_count > 0:
                    print(f"  [SAVE] Saving progress to {ITEM_MAP_PATH.name}...")
                    df.to_csv(ITEM_MAP_PATH, index=False)

                    # Also save failed items
                    if failed_items:
                        with open(FAILED_ITEMS_LOG, "w", encoding="utf-8") as f:
                            for item in failed_items:
                                f.write(f"{item['Item_ID']},{item['step']}\n")

            print(f"\n[OK] Total categorized: {total_categorized:,} items")

            # Show sample from last chunk
            if total_categorized > 0:
                print("\nSample categorized items:")
                sample_count = 0
                for result in results[:10]:  # Show up to 10 from last chunk
                    if result.get("Category_Level_1") != "Uncategorized":
                        print(
                            f"  {result['Item_ID']}: {result['Category_Level_1']} > "
                            f"{result['Category_Level_2']} > {result['Category_Level_3']} "
                            f"({result.get('extraction_confidence', 0):.0%})"
                        )
                        sample_count += 1
                        if sample_count >= 5:
                            break
    else:
        print("\n[Skipping categorization]")

    # =========================================================================
    # STEP 2: Electrical Spec Extraction
    # =========================================================================
    if not skip_electrical:
        print("\n" + "-" * 40)
        print("Step 2: Electrical Spec Extraction")
        print("-" * 40)

        # Find electrical items that haven't been through spec extraction yet
        electrical_mask = (
            df["Category_Level_1"].isin(ELECTRICAL_CATEGORIES)
            & ~df["analysis_type"].isin(["regex_extraction", "ai_extraction"])
        )

        # If reprocessing, only include failed items
        if reprocess_failed and failed_item_sets:
            failed_ids = failed_item_sets.get("electrical_extraction", set())
            if failed_ids:
                electrical_mask = electrical_mask & df["Item_ID"].astype(str).isin(failed_ids)
            else:
                electrical_mask = pd.Series([False] * len(df))

        electrical_items = df[electrical_mask].copy()

        print(f"Found {len(electrical_items):,} electrical items needing spec extraction")

        if len(electrical_items) > 0:
            if limit:
                electrical_items = electrical_items.head(limit)

            # Process in chunks to save progress incrementally
            CHUNK_SIZE = 1000
            total_items = len(electrical_items)
            total_extracted = 0

            for chunk_start in range(0, total_items, CHUNK_SIZE):
                chunk_end = min(chunk_start + CHUNK_SIZE, total_items)
                chunk = electrical_items.iloc[chunk_start:chunk_end]

                print(f"\n  Processing items {chunk_start+1:,}-{chunk_end:,} of {total_items:,}...")

                items = chunk[["Item_ID", "Raw_Description"]].to_dict("records")

                # Pre-filter: skip items with obviously invalid descriptions
                items, skipped_count = filter_invalid_items(items)
                if skipped_count > 0:
                    print(f"    Skipped {skipped_count:,} items with invalid/generic descriptions")

                results = extractor.extract_electrical_specs(items)

                # Save cache incrementally to prevent loss on crash
                if extractor._cache:
                    save_spec_cache(extractor._cache)

                # Columns that should be numeric (need safe conversion)
                numeric_cols = {"spec_amps", "spec_phase", "spec_poles", "spec_kaic", "spec_kva"}

                # Apply results using O(1) index lookup
                extracted_count = 0
                for result in results:
                    item_id = str(result.get("Item_ID", ""))
                    idx = item_index.get(item_id)
                    if idx is not None:
                        has_specs = False
                        for col in [
                            "spec_type", "spec_amps", "spec_voltage", "spec_phase",
                            "spec_poles", "spec_kaic", "spec_frame", "spec_kva",
                            "spec_gauge", "spec_nema",
                        ]:
                            if col in result and result[col] is not None:
                                value = result[col]
                                # Convert to numeric for numeric columns
                                if col in numeric_cols:
                                    value = safe_numeric(value, col)
                                if value is not None:
                                    df.at[idx, col] = value
                                    has_specs = True
                        if "extraction_confidence" in result:
                            df.at[idx, "extraction_confidence"] = result["extraction_confidence"]
                        if "analysis_type" in result:
                            df.at[idx, "analysis_type"] = result["analysis_type"]
                        if has_specs:
                            extracted_count += 1
                        else:
                            failed_items.append({"Item_ID": item_id, "step": "electrical_extraction"})

                total_extracted += extracted_count
                print(f"    Extracted specs for {extracted_count:,} items in this chunk")

                # Save progress after each chunk
                if not dry_run and extracted_count > 0:
                    print(f"    [SAVE] Saving progress to {ITEM_MAP_PATH.name}...")
                    df.to_csv(ITEM_MAP_PATH, index=False)

                # Show sample from first chunk only
                if chunk_start == 0 and extracted_count > 0:
                    print("\n  Sample extracted specs:")
                    for result in results[:5]:
                        if result.get("spec_type") or result.get("spec_amps"):
                            specs = []
                            if result.get("spec_type"):
                                specs.append(result["spec_type"])
                            if result.get("spec_amps"):
                                specs.append(f"{result['spec_amps']}A")
                            if result.get("spec_voltage"):
                                specs.append(result["spec_voltage"])
                            if result.get("spec_poles"):
                                specs.append(f"{result['spec_poles']}P")
                            print(f"    {result['Item_ID']}: {', '.join(specs)}")

            print(f"\nTotal extracted specs: {total_extracted:,} items")
    else:
        print("\n[Skipping electrical spec extraction]")

    # =========================================================================
    # STEP 3: Raw Material Spec Extraction
    # =========================================================================
    if not skip_raw_materials:
        print("\n" + "-" * 40)
        print("Step 3: Raw Material Spec Extraction")
        print("-" * 40)

        # Find raw material items that haven't been through spec extraction yet
        raw_mat_mask = (
            (df["Category_Level_1"] == RAW_MATERIAL_CATEGORY)
            & ~df["analysis_type"].isin(["regex_extraction", "ai_extraction"])
        )

        # If reprocessing, only include failed items
        if reprocess_failed and failed_item_sets:
            failed_ids = failed_item_sets.get("raw_material_extraction", set())
            if failed_ids:
                raw_mat_mask = raw_mat_mask & df["Item_ID"].astype(str).isin(failed_ids)
            else:
                raw_mat_mask = pd.Series([False] * len(df))

        raw_mat_items = df[raw_mat_mask].copy()

        print(f"Found {len(raw_mat_items):,} raw material items needing spec extraction")

        if len(raw_mat_items) > 0:
            if limit:
                raw_mat_items = raw_mat_items.head(limit)

            # Process in chunks to save progress incrementally
            CHUNK_SIZE = 1000
            total_items = len(raw_mat_items)
            total_extracted = 0

            for chunk_start in range(0, total_items, CHUNK_SIZE):
                chunk_end = min(chunk_start + CHUNK_SIZE, total_items)
                chunk = raw_mat_items.iloc[chunk_start:chunk_end]

                print(f"\n  Processing items {chunk_start+1:,}-{chunk_end:,} of {total_items:,}...")

                items = chunk[
                    ["Item_ID", "Raw_Description", "Category_Level_2"]
                ].to_dict("records")

                # Pre-filter: skip items with obviously invalid descriptions
                items, skipped_count = filter_invalid_items(items)
                if skipped_count > 0:
                    print(f"    Skipped {skipped_count:,} items with invalid/generic descriptions")

                results = extractor.extract_raw_material_specs(items)

                # Save cache incrementally to prevent loss on crash
                if extractor._cache:
                    save_spec_cache(extractor._cache)

                # Apply results using O(1) index lookup
                extracted_count = 0
                for result in results:
                    item_id = str(result.get("Item_ID", ""))
                    idx = item_index.get(item_id)
                    if idx is not None:
                        has_specs = False
                        for col in [
                            "RawMat_Finish", "RawMat_Thickness", "RawMat_Width",
                            "RawMat_Length", "RawMat_UoM", "RawMat_Weight_Lbs",
                        ]:
                            if col in result and result[col] is not None:
                                df.at[idx, col] = result[col]
                                has_specs = True
                        if "extraction_confidence" in result:
                            df.at[idx, "extraction_confidence"] = result["extraction_confidence"]
                        if "analysis_type" in result:
                            df.at[idx, "analysis_type"] = result["analysis_type"]
                        if has_specs:
                            extracted_count += 1
                        else:
                            failed_items.append({"Item_ID": item_id, "step": "raw_material_extraction"})

                total_extracted += extracted_count
                print(f"    Extracted specs for {extracted_count:,} items in this chunk")

                # Save progress after each chunk
                if not dry_run and extracted_count > 0:
                    print(f"    [SAVE] Saving progress to {ITEM_MAP_PATH.name}...")
                    df.to_csv(ITEM_MAP_PATH, index=False)

                # Show sample from first chunk only
                if chunk_start == 0 and extracted_count > 0:
                    print("\n  Sample extracted dimensions:")
                    for result in results[:5]:
                        if result.get("RawMat_Thickness"):
                            dims = f"{result.get('RawMat_Thickness')}\" x {result.get('RawMat_Width')}\" x {result.get('RawMat_Length')}\""
                            weight = result.get("RawMat_Weight_Lbs", "N/A")
                            print(f"    {result['Item_ID']}: {dims} = {weight} lbs")

            print(f"\nTotal extracted raw material specs: {total_extracted:,} items")
    else:
        print("\n[Skipping raw material spec extraction]")

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    remaining_uncategorized = (df["Category_Level_1"] == "Uncategorized").sum()
    print(f"Remaining uncategorized: {remaining_uncategorized:,}")

    electrical_with_specs = (
        df["Category_Level_1"].isin(ELECTRICAL_CATEGORIES) & df["spec_amps"].notna()
    ).sum()
    print(f"Electrical items with specs: {electrical_with_specs:,}")

    raw_mat_with_dims = (
        (df["Category_Level_1"] == RAW_MATERIAL_CATEGORY) & df["RawMat_Thickness"].notna()
    ).sum()
    print(f"Raw materials with dimensions: {raw_mat_with_dims:,}")

    # Log failed items for targeted reprocessing
    if failed_items:
        print(f"\nFailed items: {len(failed_items):,}")
        if not dry_run:
            with open(FAILED_ITEMS_LOG, "w", encoding="utf-8") as f:
                for item in failed_items:
                    f.write(f"{item['Item_ID']},{item['step']}\n")
            print(f"  Failed items logged to: {FAILED_ITEMS_LOG}")

    # Save spec cache (always, even in dry-run - cache is separate from data)
    if extractor._cache:
        save_spec_cache(extractor._cache)
        print(f"Spec cache saved ({len(extractor._cache):,} entries)")

    # Save
    if not dry_run:
        print(f"\nSaving to: {ITEM_MAP_PATH}")
        try:
            df.to_csv(ITEM_MAP_PATH, index=False)
            print("Done!")
        except Exception as e:
            print(f"ERROR saving file: {e}")
    else:
        print("\n[DRY RUN] No changes saved.")

    return df


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="AI-powered item categorization and spec extraction"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save changes, just preview",
    )
    parser.add_argument(
        "--skip-categorization",
        action="store_true",
        help="Skip AI categorization step",
    )
    parser.add_argument(
        "--skip-electrical",
        action="store_true",
        help="Skip electrical spec extraction",
    )
    parser.add_argument(
        "--skip-raw-materials",
        action="store_true",
        help="Skip raw material spec extraction",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only first N items (for testing)",
    )
    parser.add_argument(
        "--reprocess-failed",
        action="store_true",
        help="Reprocess only items from failed_items.log",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the spec extraction cache before running",
    )

    args = parser.parse_args()

    if args.clear_cache and SPEC_CACHE_PATH.exists():
        SPEC_CACHE_PATH.unlink()
        print("Spec cache cleared.")

    run_ai_enrichment(
        dry_run=args.dry_run,
        skip_categorization=args.skip_categorization,
        skip_electrical=args.skip_electrical,
        skip_raw_materials=args.skip_raw_materials,
        limit=args.limit,
        reprocess_failed=args.reprocess_failed,
    )


if __name__ == "__main__":
    main()
