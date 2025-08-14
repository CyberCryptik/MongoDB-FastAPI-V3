"""from pydantic import BaseModel
from typing import List, Dict, Any

class AggregateRequest(BaseModel):
    # added this to specify that any collection can be used (previously only the "comments" collection):
    collection: str
    pipeline: List[Dict[str, Any]]
"""

"""from pydantic import BaseModel
from typing import List, Dict, Any, Union

class AggregateRequest(BaseModel):
    collection: str
    pipeline: Union[List[Dict[str, Any]], List[str]]"""

# schemas.py
from pydantic import BaseModel
from typing import List, Dict, Any, Union, Optional

class AggregateRequest(BaseModel):
    db_name: Optional[str] = None
    collection: str
    pipeline: Union[List[Dict[str, Any]], List[str]]

class SchemaRequest(BaseModel):
    db_name: Optional[str] = None
