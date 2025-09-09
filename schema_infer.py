import os
import base64
from pymongo import MongoClient
from functools import lru_cache
from collections import defaultdict
from bson import ObjectId, Binary
from datetime import datetime, date
from typing import Any, Union, Dict

def extract_paths(doc: Union[Dict, list, Any], current_path: str = "") -> Dict:
    """Recursively extracts all field paths and their Python types from a document."""
    paths = defaultdict(set)
    if isinstance(doc, dict):
        for k, v in doc.items():
            new_path = f"{current_path}.{k}" if current_path else k
            paths[new_path].add(type(v).__name__)
            sub_paths = extract_paths(v, new_path)
            for p, t in sub_paths.items():
                paths[p].update(t)
    elif isinstance(doc, list):
        for i, v in enumerate(doc):
            sub_paths = extract_paths(v, current_path)
            for p, t in sub_paths.items():
                paths[p].update(t)
    return paths

def flatten_nested_lists_recursively(data: Any) -> Any:
    """
    Recursively traverses a document to flatten any list that contains another list.
    This ensures all lists are at a single level.
    """
    if isinstance(data, dict):
        return {k: flatten_nested_lists_recursively(v) for k, v in data.items()}
    elif isinstance(data, list):
        flattened_list = []
        has_nested_list = any(isinstance(item, list) for item in data)
        if has_nested_list:
            for item in data:
                if isinstance(item, list):
                    flattened_list.extend(item)
                else:
                    flattened_list.append(item)
            data = flattened_list
        return [flatten_nested_lists_recursively(item) for item in data]
    else:
        return data

def _safe_value(v: Any) -> Any:
    """
    Converts a value to a JSON-safe representation by handling BSON types.
    This function is crucial for normalizing the data.
    """
    if isinstance(v, dict):
        if "$numberInt" in v:
            return int(v["$numberInt"])
        if "$numberDouble" in v:
            return float(v["$numberDouble"])
        if "$numberLong" in v:
            return int(v["$numberLong"])
        if "$date" in v:
            return datetime.fromtimestamp(v["$date"] / 1000).isoformat()
        return {kk: _safe_value(vv) for kk, vv in v.items()}
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, (bytes, bytearray, Binary)):
        try:
            return base64.b64encode(bytes(v)).decode("ascii")
        except Exception:
            return repr(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, list):
        return [_safe_value(x) for x in v]
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return repr(v)

@lru_cache()
def get_schema_map_and_samples(db_name: str | None = None, sample_size: int = 50) -> dict:
    """
    Returns:
      {
        "schema":  { collection_name: { field_path: {types: [...], query_guidance: "..."} } },
        "samples": { collection_name: one_sample_doc_or_None }
      }
    """
    uri = os.getenv("MONGODB_URI")
    chosen_db_name = db_name or os.getenv("DB_NAME")
    if not uri or not chosen_db_name:
        raise RuntimeError("MONGODB_URI and DB_NAME (or db_name param) must be set")

    client = MongoClient(uri)
    db = client[chosen_db_name]

    schema_map = {}
    samples = {}

    for coll in db.list_collection_names():
        combined = defaultdict(set)
        cursor = db[coll].find().limit(sample_size)
        docs = list(cursor)
        
        for doc in docs:
            normalized_doc = flatten_nested_lists_recursively(doc)
            for path, types in extract_paths(normalized_doc).items():
                combined[path].update(types)

        current_coll_schema = {}
        for path, types in combined.items():
            field_types = sorted(list(types))
            field_info = {"types": field_types}
            if "datetime" in field_types or "date" in field_types or path == "Date":
                field_info["query_guidance"] = (
                    "For month/year filtering on this field, "
                    "prefer using $addFields with $year and $month operators. "
                    "Example: {$addFields: {docYear: {$year: '$<FIELD_NAME>'}, docMonth: {$month: '$<FIELD_NAME>'}}}, "
                    "{$match: {docYear: 2025, docMonth: 7}}."
                )
            current_coll_schema[path] = field_info
        
        schema_map[coll] = current_coll_schema
        
        sample = docs[0] if docs else None
        samples[coll] = _safe_value(flatten_nested_lists_recursively(sample)) if sample is not None else None

    return {
        "schema": schema_map,
        "samples": samples
    }
