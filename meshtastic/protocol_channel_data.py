"""Protocol handler for all packets containing channel data"""
from typing import Any
import logging

import google.protobuf.json_format
from google.protobuf.message import Message
from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import topic_map, logger
from mesh_interface import MeshInterface
from protocol_base import ProtocolHandlerBase, CHANNEL_HANDLER

from meshtastic.protobuf import mesh_pb2

logger = logging.getLogger(__name__)

@ProtocolHandlerBase.register
class ChannelHandler(ProtocolHandlerBase):
    """Protocol handler for all packets containing channel data"""
    def __init__(self, ifMesh: MeshInterface, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = CHANNEL_HANDLER

    def receivePacket(self, field: str, packet: Message) -> None:
        chanDef = google.protobuf.json_format.MessageToDict(packet).get(field, None)
        if chanDef is not None:
            chanRole = chanDef.get('role', 'NONE')
            if chanRole == 'PRIMARY':
                chanNo = 0
            else:
                chanNo = chanDef['index']
            data = {chanNo: {'role': chanRole, 'settings': chanDef['settings']}}
            pub.sendMessage(topic_map.SUBS_CHANNEL_PUB, field='channel', data=data )
        else:
            logger.debug(f"Pub-Sub: Received Invalid channel message")

    def sendPacket(self, data: dict) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass
