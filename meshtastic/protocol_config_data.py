"""Protocol handler for all packets containing configuration data
(either "normal" config or "module_config"
"""
from typing import Any
import logging

import google.protobuf.json_format
from google.protobuf.message import Message
from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import topic_map, logger
from mesh_interface import MeshInterface
from protocol_base import ProtocolHandlerBase, CONFIG_HANDLER


logger = logging.getLogger(__name__)

@ProtocolHandlerBase.register
class ConfigHandler(ProtocolHandlerBase):
    """Protocol handler for all packets containing configuration data"""
    def __init__(self, ifMesh: MeshInterface, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = CONFIG_HANDLER

    def receivePacket(self, field: str, packet: Message) -> None:
        cfgData = google.protobuf.json_format.MessageToDict(packet).get(field, None)
        if cfgData is not None:
            pub.sendMessage(topic_map.SUBS_CONFIG_PUB, field, cfgData)
        else:
            logger.debug(f"Pub-Sub: Received Invalid channel message")

    def sendPacket(self, data: dict) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass

