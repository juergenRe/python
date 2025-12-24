"""Keeps all data about the mesh entities during execution of a command.
Especially this is the local node and all discovered other nodes accessed via mesh"""

from meshtastic import Node


class MeshModel:
    """Keeps all data about the mesh entities during execution of a command."""
    def __init__(self) -> None:
        self.nodes: dict[str, dict] | None = None
        self.localNodeNum: int = 0
        self.localNode: Node | None = None


