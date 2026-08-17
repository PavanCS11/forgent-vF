"""
Vendor Enrichment Script - AI Classification

Auto-enriches vendor_map.csv entries:
Step 1: Default Intercompany to "N" for empty rows
Step 2: AI-powered standardization, classification, and location inference

Run after refresh_mappings.py to automatically enrich new vendors.

Usage:
    python enrich_vendors.py [--skip-ai] [--dry-run]
"""

import argparse
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def enrich_vendors(skip_ai: bool = False, dry_run: bool = False):
    """
    Enriches unenriched vendors.
    Step 1: Default Intercompany to "N"
    Step 2: AI enrichment for Standardized_Name, Supplier_Type, Location
    """
    if not skip_ai:
        try:
            from step_3_maintenance.c_ai_enrichment.run_vendor_enrichment import run_vendor_enrichment

            # Run vendor enrichment (handles both Intercompany defaulting and AI classification)
            run_vendor_enrichment(dry_run=dry_run)

        except ImportError as e:
            print(f"  AI enrichment module not available: {e}")
            print("  Skipping AI vendor enrichment")
            _default_intercompany(dry_run)
        except Exception as e:
            print(f"  AI vendor enrichment failed: {e}")
            print("  Falling back to Intercompany defaulting only")
            _default_intercompany(dry_run)
            raise  # Let pipeline_runner decide severity
    else:
        print("\n--- Skipping AI Vendor Enrichment (--skip-ai flag) ---")
        _default_intercompany(dry_run)


def _default_intercompany(dry_run: bool = False):
    """Fallback: just default Intercompany to 'N' without AI enrichment."""
    import pandas as pd

    vendor_map_path = os.path.join(BASE_DIR, "step_3_maintenance", "a.mappings", "vendor_map.csv")
    df = pd.read_csv(vendor_map_path, low_memory=False)

    empty_mask = df["Intercompany"].isna() | (df["Intercompany"].astype(str).str.strip() == "")
    empty_count = empty_mask.sum()

    if empty_count > 0:
        df.loc[empty_mask, "Intercompany"] = "N"
        print(f"Set Intercompany='N' for {empty_count:,} vendors")

        if not dry_run:
            df.to_csv(vendor_map_path, index=False)
            print(f"Saved vendor_map.csv")
        else:
            print("[DRY RUN] No changes saved")
    else:
        print("All vendors already have Intercompany set")


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Vendor enrichment - AI classification"
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip AI enrichment (only default Intercompany)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save changes, just preview",
    )

    args = parser.parse_args()
    enrich_vendors(skip_ai=args.skip_ai, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
