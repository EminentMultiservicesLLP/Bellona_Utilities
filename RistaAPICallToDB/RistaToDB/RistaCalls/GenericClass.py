from enum import Enum

class QueryType(Enum):
    Select = 1
    Insert = 2
    Update = 3
    Delete = 3

class executeType(Enum):
    Scalar = 1
    NonQuery = 2
    Reader = 3
