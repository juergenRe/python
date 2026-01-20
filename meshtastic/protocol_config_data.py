"""Protocol handler for all packets containing config data"""
import enum
from typing import Any
import logging

from google.protobuf.message import Message
from pubsub import pub  # type: ignore[import-untyped]

from meshtastic import topic_map
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protocol_base import ProtocolHandlerBase, formatFieldName, CONFIG_HANDLER

from meshtastic.protobuf import localonly_pb2

logger = logging.getLogger(__name__)


@ProtocolHandlerBase.register
class ConfigHandler(ProtocolHandlerBase):
    """
    Protocol handler for all packets containing config data
    Ensure, only entries contained in LocalConfig are copied
    """
    def __init__(self, ifMesh: MeshInterface, field2Topic, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = CONFIG_HANDLER
        self.field2Topic = field2Topic
        self.msgProtoFields: dict = {
            'config': self.getFieldNamesT(localonly_pb2.LocalConfig()),
            'moduleConfig': self.getFieldNamesT(localonly_pb2.LocalModuleConfig())
        }

    def getFieldNamesT(self, msg: Message) -> list[str]:
        """Extract all possible field names of a message. Transform them to snake/camel case according setup"""
        fields: list[str] = msg.DESCRIPTOR.fields_by_name.keys()
        return [formatFieldName(f) for f in fields]

    def receivePacket(self, field: str, packet: Message) -> None:
        try:
            fieldT = formatFieldName(field)
            packetData = self.messageToDict(packet).get(fieldT, None)

            msgFields = self.msgProtoFields[field]
            cfgData = {k: v for k, v in packetData.items() if k in msgFields}
            if len(cfgData) > 0:
                topicName = self.field2Topic[field]
                pub.sendMessage(topicName, field=fieldT, data=cfgData)
        except Exception as ex:
            logger.debug(f"Pub-Sub: Cannot convert {field} message to dict: Exception {ex}")

    def sendPacket(self, data: dict, **kwargs) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass
