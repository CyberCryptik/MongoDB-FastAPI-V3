"""import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv  # to load env variables

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")

client = AsyncIOMotorClient(MONGODB_URI)
db = client[DB_NAME]"""

# db.py
import os
from motor.motor_asyncio import AsyncIOMotorClient
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
    return await client.list_database_names()