"""Protocol handler for all packets containing channel data"""
import enum
from typing import Any
import logging

from google.protobuf.message import Message
from pubsub import pub  # type: ignore[import-untyped]

from meshtastic import topic_map
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protocol_base import ProtocolHandlerBase, formatFieldName, CHANNEL_HANDLER
from meshtastic.mesh_model import ROLE_PRIMARY, ROLE_SECONDARY, ROLE_NONE

from meshtastic.protobuf import mesh_pb2

logger = logging.getLogger(__name__)

@ProtocolHandlerBase.register
class ChannelHandler(ProtocolHandlerBase):
    """Protocol handler for all packets containing channel data"""
    def __init__(self, ifMesh: MeshInterface, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = CHANNEL_HANDLER

    def receivePacket(self, field: str, packet: Message) -> None:
        fieldT = formatFieldName(field)
        chanDef = self.messageToDict(packet).get(fieldT, None)
        if chanDef is not None:
            chanRole = chanDef.get('role', ROLE_NONE)
            if chanRole == ROLE_PRIMARY:
                chanNo = 0
            else:
                chanNo = chanDef['index']
            data = {chanNo: {'role': chanRole, 'settings': chanDef['settings']}}
            pub.sendMessage(topic_map.SUBS_CHANNEL_PUB, field='channel', data=data )
        else:
            logger.debug(f"Pub-Sub: Received Invalid channel message")

    def sendPacket(self, data: dict, **kwargs) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass
