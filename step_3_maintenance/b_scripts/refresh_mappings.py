import pandas as pd
import os
import sys

# Add root to python path to allow imports if needed
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
STAGING_FILE = os.path.join(BASE_DIR, "step_2_enrichment", "b.staging", "pre_cleaned_staging.csv")
VENDOR_MAP_FILE = os.path.join(BASE_DIR, "step_3_maintenance", "a.mappings", "vendor_map.csv")
ITEM_MAP_FILE = os.path.join(BASE_DIR, "step_3_maintenance", "a.mappings", "item_map.csv")


def load_csv(path):
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            print(f"Warning: {path} is empty. Treating as new.")
            return None
    return None


def get_best_description(group):
    """
    Get the best description from multiple rows for an item.
    Priority: item_description > item_display_name > item_name_mpn > line_description
    Skips empty strings, placeholder values, and values that match the item_id.
    """
    placeholders = ['', '[FILL IN DESCRIPTION]']
    item_id = str(group['item_id'].iloc[0]) if 'item_id' in group.columns else ''

    # Priority 1: item_description
    if 'item_description' in group.columns:
        for val in group['item_description'].dropna():
            val_str = str(val).strip()
            if val_str and val_str not in placeholders and val_str != item_id:
                return val_str

    # Priority 2: item_display_name
    if 'item_display_name' in group.columns:
        for val in group['item_display_name'].dropna():
            val_str = str(val).strip()
            if val_str and val_str not in placeholders and val_str != item_id:
                return val_str

    # Priority 3: item_name_mpn
    if 'item_name_mpn' in group.columns:
        for val in group['item_name_mpn'].dropna():
            val_str = str(val).strip()
            if val_str and val_str not in placeholders and val_str != item_id:
                return val_str

    # Priority 4: line_description (fallback for Epicor items with no Part Description)
    if 'line_description' in group.columns:
        for val in group['line_description'].dropna():
            val_str = str(val).strip()
            if val_str and val_str not in placeholders and val_str != item_id:
                return val_str

    return ''


def refresh_mappings():
    """
    Refreshes the vendor and item mapping tables with new entries from staging data.
    Uses new snake_case column naming convention.
    """
    print("\nRefreshing mappings...")

    # 1. Load Staging Data
    if not os.path.exists(STAGING_FILE):
        print(f"Error: Staging file not found at {STAGING_FILE}")
        return

    df_staging = pd.read_csv(STAGING_FILE, low_memory=False)
    print(f"Loaded {len(df_staging):,} rows from staging.")

    # --- Vendor Mappings ---
    print("\nProcessing Vendor Mappings...")
    df_vendor_map = load_csv(VENDOR_MAP_FILE)

    # Get all unique vendors from staging (using new snake_case column name)
    if 'vendor_name' in df_staging.columns:
        staging_vendors = df_staging['vendor_name'].dropna().unique()
    else:
        print("Warning: No 'vendor_name' column in staging.")
        staging_vendors = []

    if df_vendor_map is None or df_vendor_map.empty:
        # Create new
        existing_vendors = set()
        df_existing = pd.DataFrame(columns=['Raw_Name', 'Standardized_Name'])
    else:
        existing_vendors = set(df_vendor_map['Raw_Name'].astype(str))
        df_existing = df_vendor_map

    # Identify new vendors
    new_vendors = [v for v in staging_vendors if str(v) not in existing_vendors]

    if new_vendors:
        print(f"Found {len(new_vendors)} new vendors.")
        df_new = pd.DataFrame({'Raw_Name': new_vendors, 'Standardized_Name': ''})

        # Add other columns if they exist in existing map
        for col in df_existing.columns:
            if col not in df_new.columns:
                df_new[col] = ''

        # Concatenate: New on TOP
        df_vendor_final = pd.concat([df_new, df_existing], ignore_index=True)
    else:
        print("No new vendors found.")
        df_vendor_final = df_existing

    # Save Vendor Map
    os.makedirs(os.path.dirname(VENDOR_MAP_FILE), exist_ok=True)
    df_vendor_final.to_csv(VENDOR_MAP_FILE, index=False)
    print(f"Saved {len(df_vendor_final):,} vendors to {VENDOR_MAP_FILE}")

    # --- Item Mappings ---
    print("\nProcessing Item Mappings...")
    df_item_map = load_csv(ITEM_MAP_FILE)

    # Get unique items from staging with best description
    if 'item_id' not in df_staging.columns:
        print("Warning: No 'item_id' column in staging.")
        df_staging_items = pd.DataFrame()
    else:
        # Columns to use for description extraction (line_description is fallback for Epicor)
        desc_cols = ['item_id', 'item_description', 'item_display_name', 'item_name_mpn', 'line_description']
        available_cols = [c for c in desc_cols if c in df_staging.columns]

        # Group by item_id and get best description for each
        print("Extracting best descriptions for each item...")
        df_subset = df_staging[available_cols].copy()
        df_subset['item_id'] = df_subset['item_id'].astype(str)

        # Get best description for each item
        item_descriptions = {}
        for item_id, group in df_subset.groupby('item_id'):
            item_descriptions[item_id] = get_best_description(group)

        df_staging_items = pd.DataFrame({
            'item_id': list(item_descriptions.keys()),
            'item_description': list(item_descriptions.values())
        })
        print(f"Found {len(df_staging_items):,} unique items in staging.")

    if df_item_map is None or df_item_map.empty:
        existing_items = set()
        df_existing_items = pd.DataFrame(columns=['Item_ID', 'Raw_Description', 'Category', 'Sub_Category'])
    else:
        existing_items = set(df_item_map['Item_ID'].astype(str))
        df_existing_items = df_item_map

    if not df_staging_items.empty:
        # Filter for items NOT in existing
        df_new_items = df_staging_items[~df_staging_items['item_id'].isin(existing_items)].copy()

        if not df_new_items.empty:
            print(f"Found {len(df_new_items):,} new items to add.")

            # Prepare DataFrame - map to existing column names in item_map
            df_new_items = df_new_items.rename(columns={
                'item_id': 'Item_ID',
                'item_description': 'Raw_Description'
            })

            # Ensure required columns exist
            if 'Raw_Description' not in df_new_items.columns:
                df_new_items['Raw_Description'] = ''
            df_new_items['Category'] = ''
            df_new_items['Sub_Category'] = ''

            # Select only relevant columns
            df_new_items = df_new_items[['Item_ID', 'Raw_Description', 'Category', 'Sub_Category']]

            # Concatenate: New on TOP
            df_item_final = pd.concat([df_new_items, df_existing_items], ignore_index=True)
        else:
            print("No new items found.")
            df_item_final = df_existing_items

        # Also update existing items with empty or unhelpful descriptions
        # (includes items where Raw_Description equals Item_ID)
        empty_desc_mask = (
            df_item_final['Raw_Description'].isna() |
            (df_item_final['Raw_Description'] == '') |
            (df_item_final['Raw_Description'] == '[FILL IN DESCRIPTION]') |
            (df_item_final['Raw_Description'].astype(str) == df_item_final['Item_ID'].astype(str))
        )
        items_to_update = df_item_final[empty_desc_mask]['Item_ID'].astype(str)

        updated_count = 0
        for idx in df_item_final[empty_desc_mask].index:
            item_id = str(df_item_final.at[idx, 'Item_ID'])
            if item_id in item_descriptions and item_descriptions[item_id]:
                df_item_final.at[idx, 'Raw_Description'] = item_descriptions[item_id]
                updated_count += 1

        if updated_count > 0:
            print(f"Updated {updated_count:,} existing items with better descriptions.")
    else:
        print("No item data in staging.")
        df_item_final = df_existing_items

    # Count remaining empty or unhelpful descriptions
    empty_count = (
        df_item_final['Raw_Description'].isna() |
        (df_item_final['Raw_Description'] == '') |
        (df_item_final['Raw_Description'] == '[FILL IN DESCRIPTION]') |
        (df_item_final['Raw_Description'].astype(str) == df_item_final['Item_ID'].astype(str))
    ).sum()
    print(f"Remaining items with empty/unhelpful descriptions: {empty_count:,}")

    # Save Item Map
    df_item_final.to_csv(ITEM_MAP_FILE, index=False)
    print(f"Saved {len(df_item_final):,} items to {ITEM_MAP_FILE}")


if __name__ == "__main__":
    refresh_mappings()
