"""Protocol handler for all packets related to connecting to the radio"""
from typing import Any
import logging

from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import topic_map
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protocol_base import ProtocolHandlerBase, START_CONFIG_HANDLER

import google.protobuf.json_format
from google.protobuf.message import Message

logger = logging.getLogger(__name__)

@ProtocolHandlerBase.register
class StartConfigHandler(ProtocolHandlerBase):
    """Protocol handler for all packets issued during start up"""
    def __init__(self, ifMesh: MeshInterface, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = START_CONFIG_HANDLER
        self.configId: int | None = None
        pub.subscribe(self.onMessageStart, topic_map.SUBS_STARTCOMM_START)
        logger.debug(f'Subscribing to start config command Topic: {topic_map.SUBS_STARTCOMM_START}')

    def receivePacket(self, field: str, packet: Message) -> None:
        if field == 'config_complete_id':
            if self.configId == packet.config_complete_id:
                pub.sendMessage(topic_map.SUBS_STARTCOMM_FINISH, code='OK')
            else:
                logger.debug(f"Received orphaned config_id: {self.configId}")
        else:
            logger.debug(f"Received unknown {field} with message {packet}")

    def sendPacket(self, data: dict, **kwargs) -> Any:
        pass

    def closeHandler(self) -> Any:
        pub.unsubscribe(self.onMessageStart, topic_map.SUBS_STARTCOMM_START)

    def onMessageStart(self, timeout: int = 300) -> None:
        logger.debug(f"Pub-Sub: Received Start config message")
        self.configId = self.ifMesh.startConnection('StartConfig', timeout)

