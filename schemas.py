# schemas.py
from pydantic import BaseModel
from typing import List, Dict, Any, Union, Optional

class AggregateRequest(BaseModel):
    # db_name is optional. If not provided, it will be inferred.
    db_name: Optional[str] = None
    collection: str
    pipeline: Union[List[Dict[str, Any]], List[str]] # Supports list of dicts or list of JSON strings

class SchemaRequest(BaseModel):
    # db_name is optional. If not provided, it will default to DB_NAME from environment.
    db_name: Optional[str] = None
