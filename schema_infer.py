# schema_infer.py
# schema_infer.py
import os
import base64
from pymongo import MongoClient
from functools import lru_cache
from collections import defaultdict
from bson import ObjectId, Binary
from datetime import datetime, date
from typing import Any

def extract_paths(doc: dict, prefix: str = "") -> dict:
    """
    Recursively traverse `doc` and return a map of field_path -> set(type_names)
    """
    paths = {}
    for key, val in doc.items():
        path = f"{prefix}.{key}" if prefix else key
        typ = type(val).__name__
        paths.setdefault(path, set()).add(typ)

        # Recurse into nested dict
        if isinstance(val, dict):
            paths.update(extract_paths(val, path))

        # If list of dicts, recurse into first element
        elif isinstance(val, list) and val and isinstance(val[0], dict):
            paths.update(extract_paths(val[0], path + "[]"))

    return paths

def _safe_value(v: Any) -> Any:
    """Convert non-JSON-friendly types into JSON-safe representations."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, (bytes, bytearray, Binary)):
        # encode binary as base64 ascii string
        try:
            return base64.b64encode(bytes(v)).decode("ascii")
        except Exception:
            return repr(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {kk: _safe_value(vv) for kk, vv in v.items()}
    if isinstance(v, list):
        return [_safe_value(x) for x in v]
    # keep primitive json types as-is
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    # fallback: string representation
    return repr(v)

@lru_cache()
def get_schema_map_and_samples(db_name: str | None = None, sample_size: int = 50) -> dict:
    """
    Returns:
      {
        "schema":  { collection_name: { field_path: [type_names,…] } },
        "samples": { collection_name: one_sample_doc_or_None }
      }

    If db_name is provided, only inspects that database. Otherwise uses DB_NAME env var.
    """
    uri     = os.getenv("MONGODB_URI")
    chosen_db_name = db_name or os.getenv("DB_NAME")
    if not uri or not chosen_db_name:
        raise RuntimeError("MONGODB_URI and DB_NAME (or db_name param) must be set")

    client  = MongoClient(uri)
    db      = client[chosen_db_name]

    schema_map = {}
    samples    = {}

    for coll in db.list_collection_names():
        # Build schema_map entry
        combined = defaultdict(set)
        cursor = db[coll].find().limit(sample_size)
        for doc in cursor:
            for path, types in extract_paths(doc).items():
                combined[path].update(types)
        schema_map[coll] = {p: sorted(list(t)) for p, t in combined.items()}

        # Grab one sample document and convert to JSON-safe representation
        sample = db[coll].find_one()
        samples[coll] = _safe_value(sample) if sample is not None else None

    return {
        "schema": schema_map,
        "samples": samples
    }
