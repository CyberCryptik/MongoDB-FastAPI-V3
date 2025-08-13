from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import APIKeyHeader
from typing import List, Dict, Any, Union, Optional
from bson import ObjectId
import json
import os
import logging

from schemas import AggregateRequest
from db import get_db
from dotenv import load_dotenv
from schema_infer import get_schema_map_and_samples
from db import get_db, get_database_names 

load_dotenv()
app = FastAPI()
logging.basicConfig(level=logging.INFO)

# API key setup
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("API_KEY not set in .env")

api_key_header = APIKeyHeader(name="X-API-Key")

def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def convert_objectids(obj: Any) -> Any:
    if isinstance(obj, list):
        return [convert_objectids(v) for v in obj]
    if isinstance(obj, dict):
        return {k: convert_objectids(v) for k, v in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

def normalize_objectid(stage: Any):
    if isinstance(stage, dict):
        for k, v in list(stage.items()):
            if k == "_id" and isinstance(v, str) and ObjectId.is_valid(v):
                stage[k] = ObjectId(v)
            else:
                normalize_objectid(v)
    elif isinstance(stage, list):
        for item in stage:
            normalize_objectid(item)

@app.post("/aggregate")
async def aggregate_query(
    payload: AggregateRequest,
    api_key: str = Depends(verify_api_key)
):
    raw_pipeline = payload.pipeline

    # Parse JSON strings if pipeline stages were passed as strings
    if isinstance(raw_pipeline, list) and raw_pipeline and isinstance(raw_pipeline[0], str):
        try:
            pipeline = [json.loads(stage) for stage in raw_pipeline]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid pipeline JSON strings")
    else:
        pipeline = raw_pipeline

    if not isinstance(pipeline, list):
        raise HTTPException(status_code=400, detail="Pipeline must be a list")

    # Normalize any ObjectId strings in the pipeline
    for stage in pipeline:
        normalize_objectid(stage)

    # choose target db (support federated instance or any database)
    target_db_name = payload.db_name or os.getenv("DB_NAME")
    if not target_db_name:
        raise HTTPException(status_code=400, detail="No db_name provided and DB_NAME not set")

    try:
        target_db = get_db(target_db_name)
        collection = target_db[payload.collection]

        # --- DIAGNOSTIC CHECK 1: total docs in collection
        try:
            total_docs = await collection.count_documents({})
        except Exception as e:
            logging.warning("count_documents failed (maybe unsupported by federation): %s", e)
            total_docs = None

        # --- DIAGNOSTIC CHECK 2: if there's a $match stage, test that match with count_documents()
        match_stage = None
        for stage in pipeline:
            if isinstance(stage, dict) and "$match" in stage:
                match_stage = stage["$match"]
                break

        if match_stage is not None:
            # run a simple find/count using the same match to see if any docs exist
            try:
                match_count = await collection.count_documents(match_stage)
            except Exception as e:
                # if count_documents is unsupported on this federated source, attempt a lightweight find_one
                logging.warning("count_documents on $match failed: %s — falling back to find_one()", e)
                try:
                    found = await collection.find_one(match_stage)
                    match_count = 1 if found is not None else 0
                except Exception as e2:
                    logging.exception("find_one fallback failed: %s", e2)
                    match_count = None

            # If match_count is zero, return a helpful diagnostic instead of an opaque empty aggregation
            if match_count == 0:
                # return a clear diagnostic so the caller (or LLM) can debug/repair the filter
                debug = {
                    "collection": payload.collection,
                    "db_name": target_db_name,
                    "total_docs": total_docs,
                    "match_docs": 0,
                    "match_filter": match_stage,
                    "message": "No documents matched the pipeline's $match — check the field path, type, or value."
                }
                logging.info("Aggregate diagnostic: %s", debug)
                return {"results": [], "debug": debug}

        # All checks passed (or none applicable) -> execute aggregation
        cursor = collection.aggregate(pipeline)
        results = await cursor.to_list(length=1000)
        # Convert ObjectId in results to JSON-friendly strings
        results = convert_objectids(results)
        return {"results": results}

    except Exception as e:
        logging.exception("Error in /aggregate")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schema")
def read_schema(db_name: Optional[str] = Query(None, description="Database name (optional). If omitted uses DB_NAME from env")) -> Dict[str, Any]:
    """
    Public endpoint — no API key required.
    Returns both the inferred schema map and one sample doc per collection,
    with all non-JSONable fields pruned out.
    """
    try:
        data = get_schema_map_and_samples(db_name)
        # samples are already sanitized in schema_infer; just ensure ObjectId strings converted if needed
        return {
            "schema": data["schema"],
            "samples": data["samples"]
        }
    except Exception as e:
        logging.exception("Error in /schema")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/databases")
async def list_databases() -> List[str]:
    """
    Public endpoint to get a list of available database names.
    """
    try:
        db_names = await get_database_names()
        # Filter out system databases that aren't useful for an LLM
        return [name for name in db_names if name not in ["admin", "local", "config"]]
    except Exception as e:
        logging.exception("Error in /databases")
        raise HTTPException(status_code=500, detail=str(e))