#!/usr/bin/env python3
"""
Merge items from MetaForge API and RaidTheory arcraiders-data repo into data/items.json.

- MetaForge: primary source
- RaidTheory: overlays/extends matching items, adds new ones
- Output: stable, sorted JSON for clean Git diffs
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---- Config -----------------------------------------------------------------

METAFORGE_ITEMS_URL = "https://metaforge.app/api/arc-raiders/items"

# NOTE: RaidTheory uses an `items/` directory, not a single items.json file
RAIDTHEORY_ITEMS_DIR = Path("external/arcraiders-data/items")

OUTPUT_PATH = Path("data/items.json")


# ---- Loaders ----------------------------------------------------------------

def load_metaforge_items() -> List[Dict[str, Any]]:
    """Fetch items from MetaForge API."""
    logging.info("Fetching MetaForge items from %s", METAFORGE_ITEMS_URL)
    resp = requests.get(METAFORGE_ITEMS_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # TODO: depending on how their API is shaped (and paginated),
    # you may need to add pagination or params here.
    if isinstance(data, dict):
        for key in ("items", "data", "results"):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break
        else:
            raise ValueError("Unexpected MetaForge response shape (dict without items list)")
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError(f"Unexpected MetaForge response type: {type(data)}")

    logging.info("Loaded %d MetaForge items", len(items))
    return items


def load_raidtheory_items() -> List[Dict[str, Any]]:
    """
    Load items from the RaidTheory repo.

    We read ALL JSON files under external/arcraiders-data/items/.
    """
    if not RAIDTHEORY_ITEMS_DIR.exists():
        logging.warning(
            "RaidTheory items directory %s not found – skipping RaidTheory source",
            RAIDTHEORY_ITEMS_DIR,
        )
        return []

    items: List[Dict[str, Any]] = []

    logging.info("Scanning RaidTheory items in %s", RAIDTHEORY_ITEMS_DIR)
    for path in RAIDTHEORY_ITEMS_DIR.glob("*.json"):
        logging.info("  Reading %s", path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            items.extend(data)
        elif isinstance(data, dict):
            # Some files may be of shape { "items": [...] }
            if "items" in data and isinstance(data["items"], list):
                items.extend(data["items"])
            else:
                items.append(data)
        else:
            logging.warning("    Skipping %s – unexpected JSON type %s", path, type(data))

    logging.info("Loaded %d RaidTheory items", len(items))
    return items


# ---- Merge logic ------------------------------------------------------------

def item_key(item: Dict[str, Any]) -> str:
    """
    Derive a stable key for an item.

    Priority: id → slug → name.
    Adjust if your real data uses something else.
    """
    for field in ("id", "slug", "name"):
        value = item.get(field)
        if isinstance(value, (str, int)):
            return str(value).lower().strip()

    # Fallback: entire item as a JSON string (rare)
    return json.dumps(item, sort_keys=True)


def merge_items(
    metaforge_items: List[Dict[str, Any]],
    raidtheory_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Merge MetaForge + RaidTheory items.

    - MetaForge acts as the base record.
    - RaidTheory overlays additional/updated fields for matching items.
    - New RaidTheory-only items are added as well.
    """
    merged: Dict[str, Dict[str, Any]] = {}

    # MetaForge base
    for item in metaforge_items:
        key = item_key(item)
        merged[key] = dict(item)

    # RaidTheory overlay / additions
    for item in raidtheory_items:
        key = item_key(item)
        if key in merged:
            base = merged[key]
            # Overlay non-empty values from RaidTheory
            for k, v in item.items():
                if v not in (None, "", [], {}):
                    base[k] = v
        else:
            merged[key] = dict(item)

    items_list = list(merged.values())
    logging.info(
        "Merged %d MetaForge + %d RaidTheory items into %d unique items",
        len(metaforge_items),
        len(raidtheory_items),
        len(items_list),
    )

    # ---- Option C: stable, deterministic output -----------------------------

    def sort_key(it: Dict[str, Any]):
        name = str(it.get("name", "")).lower()
        id_val = str(it.get("id", "")).lower()
        return (name, id_val)

    items_list.sort(key=sort_key)

    normalized: List[Dict[str, Any]] = []
    for item in items_list:
        normalized.append({k: item[k] for k in sorted(item.keys())})

    return normalized


# ---- Writer -----------------------------------------------------------------

def write_items(items: List[Dict[str, Any]]) -> None:
    """Write merged items to data/items.json with pretty, stable JSON."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Writing %d items to %s", len(items), OUTPUT_PATH)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)


# ---- Main -------------------------------------------------------------------

def main() -> None:
    metaforge_items = load_metaforge_items()
    raidtheory_items = load_raidtheory_items()
    merged_items = merge_items(metaforge_items, raidtheory_items)
    write_items(merged_items)


if __name__ == "__main__":
    main()
