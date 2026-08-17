"""
Item Consolidator - Semantic Deduplication

Assigns Canonical_Name and Canonical_Group_ID to all items in item_map.csv
using a 3-pass approach:
  Pass 1: Exact normalized description match (deterministic)
  Pass 2: Spec-based fingerprint matching within category (deterministic)
  Pass 3: AI-assisted fuzzy matching for remaining singletons (targeted API calls)

Every row gets both columns. Singletons get their own group.
"""

import re
import json
import time
import random
import hashlib
import requests
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os

import pandas as pd
import numpy as np

try:
    from .taxonomy import (
        ELECTRICAL_SPEC_FIELDS,
        RAW_MATERIAL_SPEC_FIELDS,
        API_BASE_URL,
        DEFAULT_MODEL,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_CONSOLIDATION_EXACT,
        CONFIDENCE_CONSOLIDATION_FINGERPRINT,
        CONFIDENCE_CONSOLIDATION_AI,
        CONSOLIDATION_CLUSTER_SIZE,
        CONSOLIDATION_SIMILARITY_THRESHOLD,
    )
except ImportError:
    from taxonomy import (
        ELECTRICAL_SPEC_FIELDS,
        RAW_MATERIAL_SPEC_FIELDS,
        API_BASE_URL,
        DEFAULT_MODEL,
        RATE_LIMIT_DELAY,
        MAX_RETRIES,
        CONFIDENCE_CONSOLIDATION_EXACT,
        CONFIDENCE_CONSOLIDATION_FINGERPRINT,
        CONFIDENCE_CONSOLIDATION_AI,
        CONSOLIDATION_CLUSTER_SIZE,
        CONSOLIDATION_SIMILARITY_THRESHOLD,
    )

# Load .env from project root
ENV_PATH = Path(__file__).parent.parent.parent / ".env"
if ENV_PATH.exists():
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)

# Paths
ITEM_MAP_PATH = Path(__file__).parent.parent / "a.mappings" / "item_map.csv"
CACHE_PATH = Path(__file__).parent / "consolidation_cache.json"

# Spec columns used for fingerprinting
ELECTRICAL_FINGERPRINT_FIELDS = [
    "spec_type", "spec_amps", "spec_voltage", "spec_kaic", "spec_poles", "spec_frame",
]
RAWMAT_FINGERPRINT_FIELDS = [
    "RawMat_Finish", "RawMat_Thickness", "RawMat_Width", "RawMat_Length",
]


class ItemConsolidator:
    """
    3-pass item deduplication that assigns Canonical_Name and
    Canonical_Group_ID to every row in item_map.csv.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            print("  WARNING: OPENROUTER_API_KEY not found. Pass 3 (AI) will be disabled.")
        self._cache = self._load_cache()
        self._cache_hits = 0

        # Statistics
        self.stats = {
            "total_rows": 0,
            "pass1_grouped": 0,
            "pass1_groups": 0,
            "pass2_grouped": 0,
            "pass2_groups": 0,
            "pass3_grouped": 0,
            "pass3_groups": 0,
            "pass3_api_calls": 0,
            "singletons": 0,
            "total_groups": 0,
        }

    # =========================================================================
    # CACHE
    # =========================================================================

    MAX_CONSOLIDATION_CACHE = 10000

    def _load_cache(self) -> Dict:
        if CACHE_PATH.exists():
            try:
                with open(CACHE_PATH) as f:
                    cache = json.load(f)
                # Evict oldest entries if too large
                if len(cache) > self.MAX_CONSOLIDATION_CACHE:
                    evicted = len(cache) - self.MAX_CONSOLIDATION_CACHE
                    keys = list(cache.keys())
                    for key in keys[:evicted]:
                        del cache[key]
                    print(f"  Consolidation cache evicted {evicted:,} entries, kept {len(cache):,}")
                return cache
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save_cache(self):
        with open(CACHE_PATH, "w") as f:
            json.dump(self._cache, f, indent=2)

    def _cache_key(self, descriptions: List[str]) -> str:
        """Build a deterministic cache key from a sorted list of descriptions."""
        combined = "|".join(sorted(d.strip().lower() for d in descriptions))
        return "consol_" + hashlib.sha256(combined.encode()).hexdigest()[:16]

    # =========================================================================
    # DESCRIPTION NORMALIZATION
    # =========================================================================

    def _normalize_description(self, desc: str) -> str:
        """
        Normalize a description for exact-match grouping.

        Steps: lowercase, collapse whitespace, strip non-alphanumeric
        (keep /, -, . for specs), remove noise words.
        """
        if pd.isna(desc) or not str(desc).strip():
            return ""
        desc = str(desc).lower().strip()
        # Collapse whitespace
        desc = re.sub(r"\s+", " ", desc)
        # Remove trailing/leading punctuation (but keep internal spec chars)
        desc = desc.strip(".,;:!? ")
        return desc

    # =========================================================================
    # QUALITY SCORING
    # =========================================================================

    def _score_description_quality(self, row: pd.Series) -> int:
        """Score a row to pick the best Canonical_Name within a group."""
        score = 0
        desc = str(row.get("Raw_Description", ""))

        # Prefer longer descriptions (more informative), cap at 100
        score += min(len(desc), 100)

        # Prefer descriptions with spec data present
        for col in ELECTRICAL_SPEC_FIELDS + RAW_MATERIAL_SPEC_FIELDS:
            if col in row.index and pd.notna(row.get(col)) and str(row.get(col)).strip():
                score += 10

        # Prefer non-Miscellaneous categories
        cat = row.get("Category_Level_1", "")
        if cat and cat not in ("Miscellaneous/Other", "Uncategorized", "", "nan"):
            score += 50

        # Prefer higher extraction confidence
        conf = pd.to_numeric(row.get("extraction_confidence"), errors="coerce")
        if pd.notna(conf):
            score += int(conf * 30)

        # Penalize Master Data Only
        if str(row.get("Category_Level_3", "")) == "Master Data Only":
            score -= 40

        # Penalize all-caps slightly (prefer mixed-case if available)
        if desc == desc.upper() and len(desc) > 10:
            score -= 5

        return score

    def _pick_canonical_name(self, group_df: pd.DataFrame) -> str:
        """Pick the best description from a group as the canonical name."""
        scores = group_df.apply(self._score_description_quality, axis=1)
        best_idx = scores.idxmax()
        best_desc = str(group_df.loc[best_idx, "Raw_Description"]).strip()

        # Title-case if ALL CAPS, otherwise preserve
        if best_desc == best_desc.upper() and len(best_desc) > 5:
            best_desc = best_desc.title()

        return best_desc

    # =========================================================================
    # PASS 1: EXACT NORMALIZED MATCH
    # =========================================================================

    def _pass1_exact_match(self, df: pd.DataFrame) -> Tuple[Dict[int, int], Dict[int, str], int]:
        """
        Group items by exact normalized description.

        Returns:
            (group_assignments, group_names, next_group_id)
            - group_assignments: {df_index: group_id}
            - group_names: {group_id: canonical_name}
            - next_group_id: next available group ID
        """
        print("\n--- Pass 1: Exact Description Match ---")

        df["_norm_desc"] = df["Raw_Description"].apply(self._normalize_description)

        # Group by normalized description
        groups = df.groupby("_norm_desc")

        group_assignments = {}  # index -> group_id
        group_names = {}  # group_id -> canonical_name
        group_id = 1

        multi_groups = 0
        multi_items = 0

        for norm_desc, group_df in groups:
            if not norm_desc:
                continue  # Skip empty descriptions

            if len(group_df) >= 2:
                # Multi-item group
                canonical = self._pick_canonical_name(group_df)
                for idx in group_df.index:
                    group_assignments[idx] = group_id
                group_names[group_id] = canonical
                multi_groups += 1
                multi_items += len(group_df)
                group_id += 1

        print(f"  Found {multi_groups:,} description groups covering {multi_items:,} items")
        print(f"  Singletons remaining: {len(df) - multi_items:,}")

        if multi_groups > 0:
            # Show top groups
            group_sizes = df.loc[list(group_assignments.keys())].groupby("_norm_desc").size()
            top = group_sizes.nlargest(5)
            print(f"  Top groups:")
            for desc, count in top.items():
                print(f"    {count:,} items: \"{desc[:70]}\"")

        self.stats["pass1_groups"] = multi_groups
        self.stats["pass1_grouped"] = multi_items

        return group_assignments, group_names, group_id

    # =========================================================================
    # PASS 2: SPEC FINGERPRINT MATCH
    # =========================================================================

    def _build_spec_fingerprint(self, row: pd.Series) -> Optional[str]:
        """
        Build a deterministic fingerprint from spec columns.
        Returns None if insufficient data.
        """
        parts = []
        cat_l2 = str(row.get("Category_Level_2", ""))
        if not cat_l2 or cat_l2 in ("nan", "", "Uncategorized"):
            return None

        parts.append(cat_l2)

        # Electrical fingerprint
        for field in ELECTRICAL_FINGERPRINT_FIELDS:
            val = row.get(field)
            if pd.notna(val) and str(val).strip() and str(val) != "nan":
                parts.append(f"{field}:{val}")

        # Raw material fingerprint
        for field in RAWMAT_FINGERPRINT_FIELDS:
            val = row.get(field)
            if pd.notna(val) and str(val).strip() and str(val) != "nan":
                parts.append(f"{field}:{val}")

        # Need category + at least 2 spec fields to be meaningful
        if len(parts) < 3:
            return None

        return "|".join(parts)

    def _pass2_spec_fingerprint(
        self, df: pd.DataFrame, assigned: set, next_group_id: int
    ) -> Tuple[Dict[int, int], Dict[int, str], int]:
        """
        Group singletons by spec fingerprint within same category.

        Returns same format as pass1.
        """
        print("\n--- Pass 2: Spec Fingerprint Matching ---")

        singletons = df.loc[~df.index.isin(assigned)]
        print(f"  Processing {len(singletons):,} singletons")

        # Build fingerprints
        fingerprints = singletons.apply(self._build_spec_fingerprint, axis=1)
        has_fp = fingerprints.notna()
        print(f"  Built fingerprints for {has_fp.sum():,} items ({(~has_fp).sum():,} had insufficient spec data)")

        group_assignments = {}
        group_names = {}
        new_groups = 0
        new_items = 0

        if has_fp.any():
            fp_df = singletons[has_fp].copy()
            fp_df["_fingerprint"] = fingerprints[has_fp]

            fp_groups = fp_df.groupby("_fingerprint")
            for fp, group_df in fp_groups:
                if len(group_df) < 2:
                    continue

                # Build a human-readable canonical name from the fingerprint
                canonical = self._pick_canonical_name(group_df)
                for idx in group_df.index:
                    group_assignments[idx] = next_group_id
                group_names[next_group_id] = canonical
                new_groups += 1
                new_items += len(group_df)
                next_group_id += 1

        print(f"  Found {new_groups:,} fingerprint groups covering {new_items:,} items")
        remaining = len(singletons) - new_items
        print(f"  Singletons remaining: {remaining:,}")

        self.stats["pass2_groups"] = new_groups
        self.stats["pass2_grouped"] = new_items

        return group_assignments, group_names, next_group_id

    # =========================================================================
    # PASS 3: AI-ASSISTED FUZZY MATCH
    # =========================================================================

    def _token_similarity(self, desc_a: str, desc_b: str) -> float:
        """Token-based Jaccard similarity on normalized descriptions."""
        tokens_a = set(self._normalize_description(desc_a).split())
        tokens_b = set(self._normalize_description(desc_b).split())
        if not tokens_a or not tokens_b:
            return 0.0
        intersection = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        return intersection / union if union > 0 else 0.0

    def _find_candidate_clusters(self, singletons: pd.DataFrame) -> List[List[int]]:
        """
        Find candidate clusters of similar items within same Category_Level_1.
        Returns list of clusters, each cluster is a list of df indices.
        """
        # Group by Category_Level_1 first to narrow scope
        cat_col = "Category_Level_1"
        if cat_col not in singletons.columns:
            return []

        clusters = []
        categories = singletons[cat_col].fillna("").unique()

        for cat in categories:
            if not cat or cat in ("Miscellaneous/Other", "Uncategorized"):
                continue

            cat_items = singletons[singletons[cat_col] == cat]
            if len(cat_items) < 2:
                continue

            # Skip overly large categories for performance (process in O(n²))
            if len(cat_items) > 2000:
                print(f"    Skipping '{cat}' ({len(cat_items):,} items) - too large for pairwise comparison")
                continue

            descs = cat_items["Raw_Description"].fillna("").tolist()
            indices = cat_items.index.tolist()
            n = len(descs)

            # Build adjacency: track which items are similar
            assigned_to_cluster = {}  # index -> cluster_idx

            for i in range(n):
                for j in range(i + 1, n):
                    sim = self._token_similarity(descs[i], descs[j])
                    if sim >= CONSOLIDATION_SIMILARITY_THRESHOLD:
                        idx_i, idx_j = indices[i], indices[j]

                        # Merge into existing cluster or create new
                        ci = assigned_to_cluster.get(idx_i)
                        cj = assigned_to_cluster.get(idx_j)

                        if ci is not None and cj is not None:
                            if ci != cj:
                                # Merge clusters
                                for idx in clusters[cj]:
                                    assigned_to_cluster[idx] = ci
                                clusters[ci].extend(clusters[cj])
                                clusters[cj] = []
                        elif ci is not None:
                            clusters[ci].append(idx_j)
                            assigned_to_cluster[idx_j] = ci
                        elif cj is not None:
                            clusters[cj].append(idx_i)
                            assigned_to_cluster[idx_i] = cj
                        else:
                            new_idx = len(clusters)
                            clusters.append([idx_i, idx_j])
                            assigned_to_cluster[idx_i] = new_idx
                            assigned_to_cluster[idx_j] = new_idx

        # Filter: only clusters with 2+ items, cap at CONSOLIDATION_CLUSTER_SIZE
        valid = []
        for cluster in clusters:
            if len(cluster) >= 2:
                # Cap cluster size — take the first N items
                valid.append(cluster[:CONSOLIDATION_CLUSTER_SIZE])
        return valid

    def _call_consolidation_ai(self, items: List[Dict]) -> Optional[Dict]:
        """
        Ask AI if items are the same product and get canonical name.
        Returns {"same_product": bool, "canonical_name": str, "confidence": float}
        or None on failure.
        """
        items_json = json.dumps(
            [{"id": str(item["Item_ID"]), "desc": str(item["Raw_Description"])[:400]}
             for item in items],
            indent=2,
        )

        prompt = f"""You are a procurement item deduplication expert.
Determine if these items are the SAME physical product/SKU (just described differently).

## Items to Compare:
{items_json}

## Rules:
- Same product = identical function, specs, and form factor
- Different sizes/ratings = DIFFERENT products
- Brand variants of same spec = SAME product
- Accessories vs main product = DIFFERENT products

## Response Format (JSON only):
{{"same_product": true, "canonical_name": "Best descriptive name", "confidence": 0.9}}

Respond ONLY with the JSON object, no other text."""

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": DEFAULT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(API_BASE_URL, headers=headers, json=payload, timeout=60)
                response.raise_for_status()

                content = response.json()["choices"][0]["message"]["content"].strip()
                # Remove markdown fences
                if content.startswith("```"):
                    content = content.split("\n", 1)[1] if "\n" in content else content[3:]
                if content.endswith("```"):
                    content = content[:-3]

                result = json.loads(content.strip())
                return result

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    wait_time = (2 ** attempt) * 10 + random.uniform(0, 5)
                    print(f"    Rate limited, waiting {wait_time:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(wait_time)
                elif e.response.status_code >= 500:
                    time.sleep(2 ** attempt + 1)
                else:
                    break
            except (json.JSONDecodeError, KeyError):
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    break
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    break

        return None

    def _pass3_ai_fuzzy(
        self, df: pd.DataFrame, assigned: set, next_group_id: int,
        limit: Optional[int] = None
    ) -> Tuple[Dict[int, int], Dict[int, str], int]:
        """
        AI-assisted fuzzy matching for remaining singletons.
        """
        print("\n--- Pass 3: AI Fuzzy Matching ---")

        if not self.api_key:
            print("  Skipped (no API key)")
            return {}, {}, next_group_id

        singletons = df.loc[~df.index.isin(assigned)]

        # Exclude Master Data Only from fuzzy matching
        mdo_mask = singletons["Category_Level_3"].fillna("") == "Master Data Only"
        eligible = singletons[~mdo_mask]
        # Also exclude very short descriptions
        eligible = eligible[eligible["Raw_Description"].fillna("").str.len() >= 5]

        print(f"  Eligible singletons: {len(eligible):,} (excluded {mdo_mask.sum():,} MDO + {len(singletons) - len(eligible) - mdo_mask.sum():,} short descriptions)")

        print("  Finding candidate clusters...")
        clusters = self._find_candidate_clusters(eligible)
        print(f"  Generated {len(clusters):,} candidate clusters")

        if limit and len(clusters) > limit:
            clusters = clusters[:limit]
            print(f"  Limited to {limit} clusters (--limit flag)")

        group_assignments = {}
        group_names = {}
        new_groups = 0
        new_items = 0
        api_calls = 0

        for i, cluster_indices in enumerate(clusters):
            # Build items list for this cluster
            cluster_rows = df.loc[cluster_indices]
            items = [
                {"Item_ID": str(row["Item_ID"]), "Raw_Description": str(row["Raw_Description"])}
                for _, row in cluster_rows.iterrows()
            ]

            # Check cache
            descs = [item["Raw_Description"] for item in items]
            cache_key = self._cache_key(descs)

            if cache_key in self._cache:
                result = self._cache[cache_key]
                self._cache_hits += 1
            else:
                result = self._call_consolidation_ai(items)
                api_calls += 1
                if result:
                    self._cache[cache_key] = result

                # Rate limit
                if api_calls % 10 == 0:
                    print(f"    Processed {api_calls} API calls ({new_groups} groups found)...")
                time.sleep(RATE_LIMIT_DELAY)

            if result and result.get("same_product") and result.get("confidence", 0) >= 0.7:
                canonical = result.get("canonical_name", "")
                if canonical:
                    for idx in cluster_indices:
                        group_assignments[idx] = next_group_id
                    group_names[next_group_id] = canonical
                    new_groups += 1
                    new_items += len(cluster_indices)
                    next_group_id += 1

        print(f"  API calls: {api_calls:,} (cache hits: {self._cache_hits:,})")
        print(f"  Found {new_groups:,} AI-confirmed groups covering {new_items:,} items")

        self.stats["pass3_groups"] = new_groups
        self.stats["pass3_grouped"] = new_items
        self.stats["pass3_api_calls"] = api_calls

        # Save cache after pass 3
        self._save_cache()

        return group_assignments, group_names, next_group_id

    # =========================================================================
    # MAIN ORCHESTRATION
    # =========================================================================

    def run(
        self,
        skip_ai: bool = False,
        dry_run: bool = False,
        limit: Optional[int] = None,
        item_map_path: Optional[str] = None,
    ):
        """
        Run 3-pass item consolidation.

        Args:
            skip_ai: Skip Pass 3 (AI fuzzy matching)
            dry_run: Preview without saving
            limit: Limit Pass 3 to N clusters
            item_map_path: Override default item_map.csv path
        """
        path = Path(item_map_path) if item_map_path else ITEM_MAP_PATH

        print("\n" + "=" * 60)
        print("ITEM CONSOLIDATION - 3-Pass Semantic Deduplication")
        print("=" * 60)

        # Load
        df = pd.read_csv(path, low_memory=False)
        df.columns = df.columns.str.strip()
        self.stats["total_rows"] = len(df)
        print(f"\n  Loaded {len(df):,} rows from item_map.csv")

        # Check for existing columns — if present, clear them for re-run
        if "Canonical_Name" in df.columns or "Canonical_Group_ID" in df.columns:
            print("  Clearing existing Canonical_Name/Canonical_Group_ID columns (re-run)")
            df.drop(columns=["Canonical_Name", "Canonical_Group_ID"], errors="ignore", inplace=True)

        # ---- PASS 1 ----
        p1_assignments, p1_names, next_id = self._pass1_exact_match(df)

        # ---- PASS 2 ----
        assigned_so_far = set(p1_assignments.keys())
        p2_assignments, p2_names, next_id = self._pass2_spec_fingerprint(df, assigned_so_far, next_id)

        # ---- PASS 3 ----
        p3_assignments = {}
        p3_names = {}
        if not skip_ai:
            assigned_so_far = assigned_so_far | set(p2_assignments.keys())
            p3_assignments, p3_names, next_id = self._pass3_ai_fuzzy(df, assigned_so_far, next_id, limit=limit)
        else:
            print("\n--- Pass 3: AI Fuzzy Matching ---")
            print("  Skipped (--skip-ai flag)")

        # ---- ASSIGN SINGLETONS ----
        all_assignments = {**p1_assignments, **p2_assignments, **p3_assignments}
        all_names = {**p1_names, **p2_names, **p3_names}

        singleton_count = 0
        for idx in df.index:
            if idx not in all_assignments:
                desc = str(df.loc[idx, "Raw_Description"]).strip()
                if not desc or desc == "nan":
                    desc = "[No Description]"
                elif desc == desc.upper() and len(desc) > 5:
                    desc = desc.title()

                all_assignments[idx] = next_id
                all_names[next_id] = desc
                next_id += 1
                singleton_count += 1

        self.stats["singletons"] = singleton_count
        self.stats["total_groups"] = len(all_names)

        # ---- WRITE COLUMNS ----
        df["Canonical_Group_ID"] = df.index.map(all_assignments)
        df["Canonical_Name"] = df["Canonical_Group_ID"].map(all_names)

        # Clean up temp columns
        df.drop(columns=["_norm_desc"], errors="ignore", inplace=True)

        # ---- VALIDATE ----
        self._validate(df)

        # ---- SUMMARY ----
        self._print_summary()

        # ---- SAVE ----
        if not dry_run:
            df.to_csv(path, index=False)
            print(f"\n  Saved item_map.csv ({len(df):,} rows, {len(df.columns)} columns)")
        else:
            print("\n  [DRY RUN] No changes saved")

    # =========================================================================
    # VALIDATION
    # =========================================================================

    def _validate(self, df: pd.DataFrame):
        """Validate consolidation results."""
        issues = []

        if df["Canonical_Name"].isna().any():
            issues.append(f"  {df['Canonical_Name'].isna().sum()} rows missing Canonical_Name")
        if df["Canonical_Group_ID"].isna().any():
            issues.append(f"  {df['Canonical_Group_ID'].isna().sum()} rows missing Canonical_Group_ID")

        # Every group should have exactly one canonical name
        name_per_group = df.groupby("Canonical_Group_ID")["Canonical_Name"].nunique()
        inconsistent = name_per_group[name_per_group > 1]
        if len(inconsistent) > 0:
            issues.append(f"  {len(inconsistent)} groups have inconsistent Canonical_Name")

        if issues:
            print("\n  VALIDATION WARNINGS:")
            for issue in issues:
                print(issue)
        else:
            print("\n  Validation: OK")

    # =========================================================================
    # REPORTING
    # =========================================================================

    def _print_summary(self):
        s = self.stats
        total = s["total_rows"]
        grouped = s["pass1_grouped"] + s["pass2_grouped"] + s["pass3_grouped"]

        print(f"\n--- Summary ---")
        print(f"  Total canonical groups: {s['total_groups']:,} (was {total:,} unique Item_IDs)")
        if s['total_groups'] > 0:
            print(f"  Compression ratio: {total / s['total_groups']:.2f}x")
        print(f"  Pass 1 (exact):       {s['pass1_grouped']:,} items in {s['pass1_groups']:,} groups ({s['pass1_grouped']/total*100:.1f}%)")
        print(f"  Pass 2 (fingerprint): {s['pass2_grouped']:,} items in {s['pass2_groups']:,} groups ({s['pass2_grouped']/total*100:.1f}%)")
        print(f"  Pass 3 (AI fuzzy):    {s['pass3_grouped']:,} items in {s['pass3_groups']:,} groups ({s['pass3_grouped']/total*100:.1f}%)")
        print(f"  Ungrouped singletons: {s['singletons']:,} ({s['singletons']/total*100:.1f}%)")
        if s['pass3_api_calls'] > 0:
            print(f"  API calls used:       {s['pass3_api_calls']:,}")

