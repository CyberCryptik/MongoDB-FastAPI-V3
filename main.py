# main.py
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import APIKeyHeader
from typing import List, Dict, Any, Union, Optional
from bson import ObjectId, Decimal128 # Explicitly import Decimal128 for serialization
import json
import os
import logging

# Import models and database functions
from schemas import AggregateRequest, SchemaRequest
from db import get_db, get_database_names, get_collection_names, get_collection_to_db_map
from schema_infer import get_schema_map_and_samples # Used by the /schema endpoint

from dotenv import load_dotenv

# Load environment variables from .env file (should be at the very top)
load_dotenv()

# Initialize FastAPI app with custom JSON encoder for Decimal128
app = FastAPI(
    json_encoders={
        Decimal128: lambda v: str(v) # Converts Decimal128 to string for JSON serialization
    }
)

# Configure basic logging for visibility
logging.basicConfig(level=logging.INFO)

# --- API Key Setup ---
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("API_KEY not set in .env") # App won't start without API_KEY

api_key_header = APIKeyHeader(name="X-API-Key")

def verify_api_key(api_key: str = Depends(api_key_header)):
    """
    Dependency to verify the API key from the 'X-API-Key' header.
    """
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- Helper Functions for MongoDB Data Handling ---

def convert_objectids(obj: Any) -> Any:
    """
    Recursively converts ObjectId instances in results to their string representation.
    Ensures that MongoDB's ObjectId type is JSON-serializable.
    """
    if isinstance(obj, list):
        return [convert_objectids(v) for v in obj]
    if isinstance(obj, dict):
        return {k: convert_objectids(v) for k, v in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

def normalize_objectid(data: Any):
    """
    Recursively converts string representations of ObjectId to ObjectId instances.
    Specifically handles _id fields and elements within $in arrays.
    Modifies the input data in place.
    """
    if isinstance(data, dict):
        for k, v in list(data.items()): # Use list() to allow modification during iteration
            if k == "_id":
                if isinstance(v, str) and ObjectId.is_valid(v):
                    data[k] = ObjectId(v)
                elif isinstance(v, dict) and "$in" in v and isinstance(v["$in"], list):
                    # Handle {"_id": {"$in": ["string_oid1", "string_oid2"]}}
                    converted_in_values = []
                    for item in v["$in"]:
                        if isinstance(item, str) and ObjectId.is_valid(item):
                            converted_in_values.append(ObjectId(item))
                        else:
                            converted_in_values.append(item) # Keep non-ObjectId strings as is
                    data[k]["$in"] = converted_in_values
                else:
                    # For other types of _id values or nested structures under _id, recurse.
                    normalize_objectid(v)
            elif isinstance(v, (dict, list)):
                # Recurse for other nested dictionaries or lists
                normalize_objectid(v)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            # Recurse for items in the list if they are dicts or lists
            if isinstance(item, (dict, list)):
                normalize_objectid(item)
    # Primitive types (like strings that aren't part of an _id directly or $in list) are returned as is.
collection_to_db_map: Dict[str, str] = {}

# --- Startup Event Handler ---
@app.on_event("startup")
async def startup_event():
    """
    FastAPI startup event. This function runs once when the application starts.
    It builds the collection_to_db_map by inspecting available databases and collections.
    """
    global collection_to_db_map
    # Populate the global map using the async function from db.py
    # Use update to merge the temporary map into the global one.
    temp_map = await get_collection_to_db_map()
    collection_to_db_map.update(temp_map)
    logging.info(f"Collection-to-db map built with {len(collection_to_db_map)} entries.")

# --- API Endpoints ---

@app.post("/aggregate")
async def aggregate_query(
    payload: AggregateRequest,
    api_key: str = Depends(verify_api_key) # API key is required for this endpoint
) -> Dict[str, Any]:
    """
    Runs a MongoDB aggregation pipeline on the specified database and collection.
    Automatically infers the database name if not provided, based on the collection name.
    Includes diagnostic checks to help debug empty results from $match stages.
    """
    raw_pipeline = payload.pipeline

    # Handle pipeline stages passed as JSON strings (for flexibility)
    if isinstance(raw_pipeline, list) and raw_pipeline and isinstance(raw_pipeline[0], str):
        try:
            pipeline = [json.loads(stage) for stage in raw_pipeline]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid pipeline JSON strings")
    else:
        pipeline = raw_pipeline

    # Ensure the pipeline is a list (as expected by MongoDB aggregation)
    if not isinstance(pipeline, list):
        raise HTTPException(status_code=400, detail="Pipeline must be a list")

    # Normalize ObjectId strings within the pipeline for correct query execution
    for stage in pipeline:
        normalize_objectid(stage)

    # --- Determine Target Database Name ---
    # Attempt to use the db_name provided in the payload first
    target_db_name = payload.db_name
    collection_name = payload.collection

    # If db_name is not provided, try to infer it from the collection_to_db_map
    if not target_db_name:
        if collection_name in collection_to_db_map:
            target_db_name = collection_to_db_map[collection_name]
        else:
            # If inference fails, and DB_NAME environment variable is not set,
            # or if the collection isn't in the map, raise an error.
            if os.getenv("DB_NAME"):
                # Fallback to default DB_NAME from env if inference fails
                # and a default is available, though this might not be the intent.
                # Consider if you truly want this fallback or prefer strict inference.
                target_db_name = os.getenv("DB_NAME")
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Database for collection '{collection_name}' could not be inferred. Please provide 'db_name' explicitly or ensure a default 'DB_NAME' is set in your environment."
                )
    
    # Final check: if after all logic, target_db_name is still None (unlikely with above changes but for safety)
    if not target_db_name:
        raise HTTPException(status_code=500, detail="Failed to determine target database name.")


    try:
        # Get the MongoDB database instance
        target_db = get_db(target_db_name)
        # Access the specified collection
        collection = target_db[collection_name]

        # --- DIAGNOSTIC CHECK 1: Total documents in collection ---
        total_docs = None
        try:
            total_docs = await collection.count_documents({})
        except Exception as e:
            logging.warning("count_documents failed (maybe unsupported by federation): %s", e)

        # --- DIAGNOSTIC CHECK 2: If a $match stage exists, test its effectiveness ---
        match_stage = None
        for stage in pipeline:
            if isinstance(stage, dict) and "$match" in stage:
                match_stage = stage["$match"]
                break

        if match_stage is not None:
            match_count = None
            try:
                match_count = await collection.count_documents(match_stage)
            except Exception as e:
                logging.warning("count_documents on $match failed: %s — falling back to find_one()", e)
                try:
                    found = await collection.find_one(match_stage)
                    match_count = 1 if found is not None else 0
                except Exception as e2:
                    logging.exception("find_one fallback failed: %s", e2)
                    match_count = None

            # If the match filter returned no documents, provide a helpful debug message
            if match_count == 0:
                debug_info = {
                    "collection": collection_name,
                    "db_name": target_db_name,
                    "total_docs": total_docs,
                    "match_docs": 0,
                    "match_filter": match_stage,
                    "message": "No documents matched the pipeline's $match — check the field path, type, or value."
                }
                logging.info("Aggregate diagnostic: %s", debug_info)
                return {"results": [], "debug": debug_info}

        # --- Execute Aggregation Pipeline ---
        cursor = collection.aggregate(pipeline)
        results = await cursor.to_list(length=None) # Retrieve all results

        # Convert ObjectIds in the results to strings for JSON serialization
        results = convert_objectids(results)
        return {"results": results}

    except Exception as e:
        # Catch and log any exceptions during the aggregation process
        logging.exception("Error in /aggregate")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/schema")
def read_schema(payload: SchemaRequest) -> Dict[str, Any]:
    """
    Public endpoint to get the inferred schema and sample documents for a database.
    Database name is provided in the request body.
    """
    try:
        data = get_schema_map_and_samples(payload.db_name)
        return {
            "schema": data["schema"],
            "samples": data["samples"]
        }
    except Exception as e:
        logging.exception("Error in /schema")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/databases")
async def list_databases_with_collections() -> Dict[str, List[str]]:
    """
    Public endpoint to list all available database names and their collections.
    Provides an overview of the data structure for external tools/LLMs.
    """
    try:
        db_names = await get_database_names()
        result = {}
        for db_name in db_names:
            collection_names = await get_collection_names(db_name)
            result[db_name] = collection_names
        return result
    except Exception as e:
        logging.exception("Error in /databases")
        raise HTTPException(status_code=500, detail=str(e))
