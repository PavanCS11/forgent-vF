"""
Vendor Enrichment Runner
========================

Standalone script to run AI-powered vendor enrichment.
Can be run independently from the main pipeline.

Enriches vendor_map.csv with:
- Standardized_Name: Cleaned vendor name
- Intercompany: Defaults to "N" for empty rows
- Supplier_Type: Manufacturer, Distributor, Service Provider, or Individual
- Location: Country-level geographic location (short value)

Usage:
    python run_vendor_enrichment.py [options]

Options:
    --dry-run              Don't save changes, just preview
    --skip-standardization Skip AI name standardization
    --skip-classification  Skip AI supplier type classification
    --skip-location        Skip AI location inference
    --limit N              Process only first N unenriched vendors
    --reprocess-failed     Reprocess only vendors from failed_vendors.log
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add parent directories to path for imports
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
sys.path.insert(0, str(SCRIPT_DIR))

from vendor_classifier import VendorClassifier
from vendor_taxonomy import clean_vendor_name_regex

# Path configuration
SCRIPT_DIR = Path(__file__).parent
MAPPINGS_DIR = SCRIPT_DIR.parent / "a.mappings"
VENDOR_MAP_PATH = MAPPINGS_DIR / "vendor_map.csv"
FAILED_VENDORS_LOG = SCRIPT_DIR / "failed_vendors.log"


def build_vendor_index(df) -> dict:
    """Build reverse index for O(1) Raw_Name -> row index lookups."""
    return {str(raw_name): idx for idx, raw_name in enumerate(df["Raw_Name"])}


def load_failed_vendors() -> set:
    """Load failed vendor names from log file."""
    failed = set()
    if FAILED_VENDORS_LOG.exists():
        with open(FAILED_VENDORS_LOG) as f:
            for line in f:
                line = line.strip()
                if "," in line:
                    raw_name, _step = line.rsplit(",", 1)
                    failed.add(raw_name)
                elif line:
                    failed.add(line)
    return failed


def run_vendor_enrichment(
    dry_run: bool = False,
    skip_standardization: bool = False,
    skip_classification: bool = False,
    skip_location: bool = False,
    limit: int = None,
    reprocess_failed: bool = False,
    clear_unknown_locations: bool = False,
):
    """
    Run vendor enrichment on vendor_map.csv.

    Args:
        dry_run: If True, don't save changes.
        skip_standardization: Skip AI name standardization.
        skip_classification: Skip AI supplier type classification.
        skip_location: Skip AI location inference.
        limit: Process only first N unenriched vendors (for testing).
        reprocess_failed: If True, only reprocess vendors from failed_vendors.log.
        clear_unknown_locations: If True, clear 'Unknown' Location values for re-enrichment.
    """
    # Load failed vendors if reprocessing
    failed_vendor_names = None
    if reprocess_failed:
        failed_vendor_names = load_failed_vendors()
        if not failed_vendor_names:
            print("No failed vendors to reprocess.")
            return
        print(f"[REPROCESS MODE] Found {len(failed_vendor_names)} failed vendors to reprocess")

    print("\n" + "=" * 60)
    print("VENDOR ENRICHMENT - AI Classification")
    print("=" * 60)

    if dry_run:
        print("[DRY RUN MODE - No changes will be saved]")

    # Load vendor map
    print(f"\nLoading vendor map from: {VENDOR_MAP_PATH}")
    df = pd.read_csv(VENDOR_MAP_PATH, low_memory=False)
    print(f"Total vendors: {len(df):,}")

    # Clear "Unknown" Location values if requested
    if clear_unknown_locations:
        unknown_mask = df["Location"].astype(str).str.strip() == "Unknown"
        count = unknown_mask.sum()
        if count > 0:
            df.loc[unknown_mask, "Location"] = ""
            print(f"Cleared {count:,} 'Unknown' Location values for re-enrichment")

    # Build index for O(1) lookups
    vendor_index = build_vendor_index(df)
    print(f"Built vendor index ({len(vendor_index):,} vendors)")

    # Track failed vendors for logging
    failed_vendors = []

    # =========================================================================
    # STEP 1: Default Intercompany to "N" (no AI needed)
    # =========================================================================
    print("\n" + "-" * 40)
    print("Step 1: Intercompany Defaulting")
    print("-" * 40)

    intercompany_empty_mask = (
        df["Intercompany"].isna() | (df["Intercompany"].astype(str).str.strip() == "")
    )
    intercompany_empty_count = intercompany_empty_mask.sum()

    if intercompany_empty_count > 0:
        df.loc[intercompany_empty_mask, "Intercompany"] = "N"
        print(f"Set Intercompany='N' for {intercompany_empty_count:,} vendors")
    else:
        print("All vendors already have Intercompany set")

    # =========================================================================
    # STEP 2: AI Classification (Standardized_Name + Supplier_Type + Location)
    # =========================================================================
    # Determine which fields to enrich based on skip flags
    skip_all_ai = skip_standardization and skip_classification and skip_location

    if not skip_all_ai:
        print("\n" + "-" * 40)
        print("Step 2: AI Vendor Classification")
        print("-" * 40)

        # Build mask for vendors needing enrichment
        needs_enrichment = pd.Series([False] * len(df), index=df.index)

        if not skip_standardization:
            needs_enrichment |= (
                df["Standardized_Name"].isna()
                | (df["Standardized_Name"].astype(str).str.strip() == "")
            )
        if not skip_classification:
            needs_enrichment |= (
                df["Supplier_Type"].isna()
                | (df["Supplier_Type"].astype(str).str.strip() == "")
            )
        if not skip_location:
            needs_enrichment |= (
                df["Location"].isna()
                | (df["Location"].astype(str).str.strip() == "")
            )

        # If reprocessing, only include failed vendors
        if reprocess_failed and failed_vendor_names:
            needs_enrichment &= df["Raw_Name"].astype(str).isin(failed_vendor_names)

        vendors_to_enrich = df[needs_enrichment].copy()
        print(f"Found {len(vendors_to_enrich):,} vendors needing enrichment")

        if len(vendors_to_enrich) > 0:
            if limit:
                vendors_to_enrich = vendors_to_enrich.head(limit)
                print(f"Processing first {limit} vendors (--limit flag)")

            # Prepare vendor list for AI
            vendor_list = vendors_to_enrich[["Raw_Name"]].to_dict("records")

            # Initialize classifier
            try:
                classifier = VendorClassifier()
                print("API connection initialized")
            except ValueError as e:
                print(f"ERROR: {e}")
                print("Please ensure OPENROUTER_API_KEY is set in .env file")
                # Still save Intercompany defaults
                if not dry_run and intercompany_empty_count > 0:
                    _save_vendor_map(df)
                return

            # Run classification
            results = classifier.classify_all(vendor_list)

            # Apply results to dataframe using O(1) index lookup
            enriched_count = 0
            for result in results:
                raw_name = str(result.get("Raw_Name", ""))
                idx = vendor_index.get(raw_name)
                if idx is None:
                    continue

                is_failed = result.get("vendor_analysis_type") == "ai_vendor_classification_failed"
                if is_failed:
                    failed_vendors.append({"Raw_Name": raw_name, "step": "classification"})
                    continue

                updated = False

                # Only update fields that are currently empty (never overwrite)
                if not skip_standardization:
                    current_std = df.at[idx, "Standardized_Name"]
                    if pd.isna(current_std) or str(current_std).strip() == "":
                        new_val = result.get("Standardized_Name", "")
                        if new_val:
                            df.at[idx, "Standardized_Name"] = new_val
                            updated = True

                if not skip_classification:
                    current_type = df.at[idx, "Supplier_Type"]
                    if pd.isna(current_type) or str(current_type).strip() == "":
                        new_val = result.get("Supplier_Type", "")
                        if new_val:
                            df.at[idx, "Supplier_Type"] = new_val
                            updated = True

                if not skip_location:
                    current_loc = df.at[idx, "Location"]
                    if pd.isna(current_loc) or str(current_loc).strip() == "":
                        new_val = result.get("Location", "")
                        if new_val:
                            df.at[idx, "Location"] = new_val
                            updated = True

                if updated:
                    enriched_count += 1

            print(f"\nEnriched: {enriched_count:,} vendors")

            # Show sample
            if enriched_count > 0:
                print("\nSample enriched vendors:")
                sample_count = 0
                for result in results[:10]:
                    if result.get("vendor_analysis_type") != "ai_vendor_classification_failed":
                        print(
                            f"  {result.get('Raw_Name', '')[:40]:40s} -> "
                            f"{result.get('Standardized_Name', '')[:25]:25s} | "
                            f"{result.get('Supplier_Type', ''):20s} | "
                            f"{result.get('Location', '')}"
                        )
                        sample_count += 1
                        if sample_count >= 5:
                            break
        else:
            print("No vendors need enrichment")
    else:
        print("\n[Skipping all AI classification (all skip flags set)]")

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    std_populated = df["Standardized_Name"].notna() & (df["Standardized_Name"].astype(str).str.strip() != "")
    type_populated = df["Supplier_Type"].notna() & (df["Supplier_Type"].astype(str).str.strip() != "")
    loc_populated = df["Location"].notna() & (df["Location"].astype(str).str.strip() != "")
    ic_populated = df["Intercompany"].notna() & (df["Intercompany"].astype(str).str.strip() != "")

    print(f"Standardized_Name: {std_populated.sum():,}/{len(df):,} populated")
    print(f"Intercompany:      {ic_populated.sum():,}/{len(df):,} populated")
    print(f"Supplier_Type:     {type_populated.sum():,}/{len(df):,} populated")
    print(f"Location:          {loc_populated.sum():,}/{len(df):,} populated")

    # Log failed vendors for targeted reprocessing
    if failed_vendors:
        print(f"\nFailed vendors: {len(failed_vendors):,}")
        if not dry_run:
            with open(FAILED_VENDORS_LOG, "w") as f:
                for vendor in failed_vendors:
                    f.write(f"{vendor['Raw_Name']},{vendor['step']}\n")
            print(f"  Failed vendors logged to: {FAILED_VENDORS_LOG}")

    # Save
    if not dry_run:
        _save_vendor_map(df)
    else:
        print("\n[DRY RUN] No changes saved.")

    return df


def _save_vendor_map(df):
    """Save vendor map with error handling and backup."""
    print(f"\nSaving to: {VENDOR_MAP_PATH}")
    try:
        df.to_csv(VENDOR_MAP_PATH, index=False)
        print("Done!")
    except Exception as e:
        print(f"ERROR saving file: {e}")
        backup_path = VENDOR_MAP_PATH.with_suffix(".backup.csv")
        df.to_csv(backup_path, index=False)
        print(f"  Saved backup to: {backup_path}")


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="AI-powered vendor enrichment (standardization, classification, location)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save changes, just preview",
    )
    parser.add_argument(
        "--skip-standardization",
        action="store_true",
        help="Skip AI name standardization",
    )
    parser.add_argument(
        "--skip-classification",
        action="store_true",
        help="Skip AI supplier type classification",
    )
    parser.add_argument(
        "--skip-location",
        action="store_true",
        help="Skip AI location inference",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only first N unenriched vendors (for testing)",
    )
    parser.add_argument(
        "--reprocess-failed",
        action="store_true",
        help="Reprocess only vendors from failed_vendors.log",
    )
    parser.add_argument(
        "--clear-unknown-locations",
        action="store_true",
        help="Clear 'Unknown' Location values to allow re-enrichment",
    )

    args = parser.parse_args()

    run_vendor_enrichment(
        dry_run=args.dry_run,
        skip_standardization=args.skip_standardization,
        skip_classification=args.skip_classification,
        skip_location=args.skip_location,
        limit=args.limit,
        reprocess_failed=args.reprocess_failed,
        clear_unknown_locations=args.clear_unknown_locations,
    )


if __name__ == "__main__":
    main()
