# db.py
import os
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List
import logging
from dotenv import load_dotenv  # to load env variables

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")

# async client used by main endpoints (motor)
client = AsyncIOMotorClient(MONGODB_URI)

def get_db(name: str | None = None):
    """
    Return an AsyncIOMotorDatabase for the given name, or the default DB_NAME.
    """
    db_name = name or DB_NAME
    if not db_name:
        raise RuntimeError("DB_NAME not set in environment and no name provided")
    return client[db_name]

async def get_database_names():
    """
    Lists all database names available in the MongoDB instance, excluding system databases.
    """
    return await client.list_database_names()

async def get_collection_names(db_name: str) -> List[str]:
    """
    Lists all collection names within a specified database.
    """
    db = client[db_name]
    return await db.list_collection_names()

async def get_collection_to_db_map():
    """
    Builds a map from collection names to database names at application startup.
    This allows inferring the database from just the collection name.
    """
    logging.info("Building collection-to-db map...")
    db_names = await get_database_names()
    local_map = {}
    for db_name in db_names:
        collections = await get_collection_names(db_name)
        for collection_name in collections:
            local_map[collection_name] = db_name
    return local_map

