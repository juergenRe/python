"""Protocol handler for all packets issued during start up"""

from typing import Any

from mesh_interface import MeshInterface
from protocol_base import IProtocolHandler, ProtocolHandlerBase, START_CONFIG_HANDLER

@ProtocolHandlerBase.register
class StartConfigHandler(ProtocolHandlerBase):
    """Protocol handler for all packets issued during start up"""
    def __init__(self, ifMesh: MeshInterface) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = START_CONFIG_HANDLER

    def receivePacket(self, address: str) -> Any:
        pass

    def sendPacket(self, address: str) -> Any:
        pass

    def addHandler(self, address: str) -> Any:
        pass

    def removeHandler(self, address: str) -> Any:
        pass

