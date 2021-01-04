from enum import Enum, auto


class RetrievalType(Enum):
    FULL_RETRIEVAL = auto()
    BLOCK_DATA_ONLY = auto()
    OUTLINE_ONLY = auto()


class DataStructure(Enum):
    TRANSACTION = 'transaction'
    BLOCK = 'block'
