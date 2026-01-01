"""Protocol handler for all packets issued during start up"""

from typing import Any
import logging

from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import topic_map, logger
from mesh_interface import MeshInterface
from protocol_base import IProtocolHandler, ProtocolHandlerBase, START_CONFIG_HANDLER

logger = logging.getLogger(__name__)

@ProtocolHandlerBase.register
class StartConfigHandler(ProtocolHandlerBase):
    """Protocol handler for all packets issued during start up"""
    def __init__(self, ifMesh: MeshInterface) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = START_CONFIG_HANDLER
        pub.subscribe(self.onMessageStart, topic_map.SUBS_STARTCOMM_START)
        logger.debug(f'Subscribing to start config command Topic: {topic_map.SUBS_STARTCOMM_START}')

    def receivePacket(self, packet) -> None:
        logger.debug(f"Received {packet}")

    def sendPacket(self, data: dict) -> Any:
        pass

    def addHandler(self, address: str) -> Any:
        pass

    def removeHandler(self, address: str) -> Any:
        pass

    def onMessageStart(self, timeout: int = 300) -> None:
        logger.debug(f"Pub-Sub: Received Start config message")
        self.ifMesh.startConnection('StartConfig', timeout)

