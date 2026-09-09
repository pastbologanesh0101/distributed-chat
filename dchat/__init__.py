"""dchat — a fault-tolerant distributed chat server built on Raft consensus."""

from .cluster import SimCluster
from .raft import RaftNode, Role
from .server import ChatNode
from .statemachine import ChatState

__version__ = "0.1.0"
__all__ = ["RaftNode", "Role", "ChatNode", "ChatState", "SimCluster"]
