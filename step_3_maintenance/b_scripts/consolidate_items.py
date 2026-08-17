"""
Item Consolidation Script - Semantic Deduplication

Assigns Canonical_Name and Canonical_Group_ID to all items in item_map.csv
using a 3-pass approach: exact match, spec fingerprint, AI fuzzy match.

Run after clean_item_map.py and before assembly.

Usage:
    python consolidate_items.py [--skip-ai] [--dry-run] [--limit N]
"""

import argparse
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def consolidate_items(skip_ai: bool = False, dry_run: bool = False, limit: int = None):
    """
    Consolidates items by assigning Canonical_Name and Canonical_Group_ID.
    Called by pipeline_runner.py in Phase 3 after clean_item_map.
    """
    try:
        from step_3_maintenance.c_ai_enrichment.item_consolidator import ItemConsolidator
    except ImportError:
        try:
            import sys
            sys.path.insert(0, BASE_DIR)
            from step_3_maintenance.c_ai_enrichment.item_consolidator import ItemConsolidator
        except ImportError as e:
            print(f"  Item consolidation module not available: {e}")
            return

    consolidator = ItemConsolidator()
    consolidator.run(skip_ai=skip_ai, dry_run=dry_run, limit=limit)


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Item consolidation - semantic deduplication"
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip Pass 3 (AI fuzzy matching), only run deterministic passes",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save changes, just preview",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit Pass 3 to N candidate clusters (for testing)",
    )

    args = parser.parse_args()
    consolidate_items(skip_ai=args.skip_ai, dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
