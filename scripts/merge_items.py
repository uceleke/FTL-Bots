#!/usr/bin/env python3
"""
Merge MetaForge items.json with RaidTheory item data.

- Reads MetaForge items from:   data/items.json
- Reads RaidTheory items from: external/arcraiders-data/items/*.json
- Enriches each MetaForge item with optional RaidTheory fields:
    rtId, weightKg, stackSize, recipe, recyclesInto, salvagesInto,
    craftBench, rtEffects
- Writes the updated data back to data/items.json (in-place).

Run locally with:
    python scripts/merge_items.py
"""

import json
import pathlib
from datetime import datetime

# ---------- Paths ----------

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
ITEMS_PATH = REPO_ROOT / "data" / "items.json"
RAID_ITEMS_DIR = REPO_ROOT / "external" / "arcraiders-data" / "items"


# ---------- Helpers ----------

def norm_id(value: str) -> str:
    """Normalize IDs like 'anvil-i' / 'Anvil I' / 'anvil_i' → 'anvil_i'."""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip().lower()
    # Replace spaces and dashes with underscores
    for ch in (" ", "-"):
        value = value.replace(ch, "_")
    return value


def norm_name(value: str) -> str:
    """Normalize names like 'Anvil I' → 'anvili' for fuzzy matching."""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip().lower()
    return "".join(ch for ch in value if ch.isalnum())


def load_metaforge_items():
    if not ITEMS_PATH.exists():
        raise SystemExit(f"[ERROR] MetaForge items.json not found: {ITEMS_PATH}")

    with ITEMS_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Expect either:
    #  - {"items": [...]}
    #  - or a bare list [...]
    if isinstance(data, dict):
        items = data.get("items")
        if items is None or not isinstance(items, list):
            raise SystemExit("[ERROR] data/items.json: expected key 'items' with a list.")
    elif isinstance(data, list):
        # Fallback: treat root as the list
        items = data
        data = {"items": items}
    else:
        raise SystemExit("[ERROR] data/items.json has unexpected structure.")

    return data, items


def load_raid_items():
    if not RAID_ITEMS_DIR.exists():
        raise SystemExit(
            f"[ERROR] RaidTheory items directory not found: {RAID_ITEMS_DIR}\n"
            "Create this path and clone/copy the arcraiders-data repo into it.\n"
            "Expected: external/arcraiders-data/items/*.json"
        )

    raid_items = []
    for path in sorted(RAID_ITEMS_DIR.glob("*.json")):
        try:
            with path.open("r", encoding="utf-8") as f:
                item = json.load(f)
            if isinstance(item, dict) and "id" in item:
                raid_items.append(item)
        except Exception as e:
            print(f"[WARN] Failed to parse RaidTheory item {path.name}: {e}")

    if not raid_items:
        print("[WARN] No RaidTheory item JSON files were loaded.")
    else:
        print(f"[INFO] Loaded {len(raid_items)} RaidTheory items.")

    return raid_items


def build_raid_indexes(raid_items):
    by_id_norm = {}
    by_name_norm = {}

    for rt in raid_items:
        rt_id = str(rt.get("id", "")).strip()
        id_key = norm_id(rt_id) if rt_id else None

        name_en = None
        name_field = rt.get("name")
        if isinstance(name_field, dict):
            name_en = name_field.get("en")
        elif isinstance(name_field, str):
            name_en = name_field

        name_key = norm_name(name_en) if name_en else None

        if id_key:
            # If duplicate IDs exist, last one wins – fine for our use case.
            by_id_norm[id_key] = rt
        if name_key and name_key not in by_name_norm:
            by_name_norm[name_key] = rt

    print(f"[INFO] RaidTheory index sizes: by_id={len(by_id_norm)}, by_name={len(by_name_norm)}")
    return by_id_norm, by_name_norm


def flatten_effects(effects_obj):
    """
    RaidTheory 'effects' looks like:
      "effects": {
        "Durability": { ..., "value": "100/100" },
        "Magazine Size": { ..., "value": 6 }
      }

    We turn that into:
      {"Durability": "100/100", "Magazine Size": 6}
    """
    if not isinstance(effects_obj, dict):
        return None

    flat = {}
    for key, val in effects_obj.items():
        if isinstance(val, dict) and "value" in val:
            flat[key] = val["value"]
    return flat or None


def enrich_item(meta_item, rt_item):
    """
    Copy selected RaidTheory fields into the MetaForge item.
    We do NOT overwrite MetaForge's core fields (name, description, type, rarity, value).
    """
    # Keep track of origin mapping
    meta_item["rtId"] = rt_item.get("id")

    # Simple scalar / structured fields we want to copy if present
    for field in ("weightKg", "stackSize", "recipe", "recyclesInto",
                  "salvagesInto", "craftBench"):
        if field in rt_item:
            meta_item[field] = rt_item[field]

    # Flatten effects into rtEffects
    rt_effects = flatten_effects(rt_item.get("effects"))
    if rt_effects is not None:
        meta_item["rtEffects"] = rt_effects


def find_matching_rt(meta_item, by_id_norm, by_name_norm):
    """
    Try to find a RaidTheory item for this MetaForge item using:
      1) normalized ID
      2) normalized English name (fallback)
    """
    meta_id = meta_item.get("id")
    id_key = norm_id(meta_id) if meta_id else None

    # Prefer ID match
    if id_key and id_key in by_id_norm:
        return by_id_norm[id_key]

    # Fallback: name match
    name = meta_item.get("name")
    if isinstance(name, dict):
        name = name.get("en")
    name_key = norm_name(name) if name else None

    if name_key and name_key in by_name_norm:
        return by_name_norm[name_key]

    return None


def main():
    print(f"[INFO] Repo root: {REPO_ROOT}")
    print(f"[INFO] MetaForge items: {ITEMS_PATH}")
    print(f"[INFO] RaidTheory items dir: {RAID_ITEMS_DIR}")

    # Load sources
    meta_root, meta_items = load_metaforge_items()
    raid_items = load_raid_items()

    by_id_norm, by_name_norm = build_raid_indexes(raid_items)

    enriched_count = 0
    total = len(meta_items)

    for item in meta_items:
        rt_match = find_matching_rt(item, by_id_norm, by_name_norm)
        if rt_match is None:
            continue
        enrich_item(item, rt_match)
        enriched_count += 1

    print(f"[INFO] Enriched {enriched_count}/{total} items from RaidTheory.")

    # Update top-level metadata timestamp to reflect enrichment
    if isinstance(meta_root, dict):
        meta_root["lastUpdatedRaidTheory"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Write back to data/items.json (pretty-printed for git diffs)
    with ITEMS_PATH.open("w", encoding="utf-8") as f:
        json.dump(meta_root, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"[INFO] Wrote merged items to {ITEMS_PATH}")


if __name__ == "__main__":
    main()