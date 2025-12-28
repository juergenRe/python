"""Protocol handler for all packets issued during start up"""

from typing import Any

from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import topic_map
from mesh_interface import MeshInterface
from protocol_base import IProtocolHandler, ProtocolHandlerBase, START_CONFIG_HANDLER


@ProtocolHandlerBase.register
class StartConfigHandler(ProtocolHandlerBase):
    """Protocol handler for all packets issued during start up"""
    def __init__(self, ifMesh: MeshInterface) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = START_CONFIG_HANDLER
        pub.subscribe(topic_map.SUBS_STARTCOMM_START)

    def receivePacket(self, packet) -> None:
        pass

    def sendPacket(self, data: dict) -> Any:
        pass

    def addHandler(self, address: str) -> Any:
        pass

    def removeHandler(self, address: str) -> Any:
        pass

