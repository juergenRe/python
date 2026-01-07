"""Handling of protocols: Interfacedefinition and ProtocolHandlerManager"""

import abc
import logging
from typing import Any, Callable
from pathlib import Path

from google.protobuf.message import Message
import google.protobuf.json_format

from pubsub import pub  # type: ignore[import-untyped]

from meshtastic import topic_map, logger
from meshtastic.protocol_interface import IProtocolHandler
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protobuf import mesh_pb2

# Name definitions for protocol handler types
UNKNOWN_HANDLER = 'Unknown'
DEFAULT_HANDLER = 'Default'
START_CONFIG_HANDLER = 'StartConfig'
LOGGING_HANDLER = 'Logging'
CHANNEL_HANDLER = 'ChannelData'
CONFIG_HANDLER = 'ConfigHandler'


class ProtocolHandlerBase(IProtocolHandler):
    """The protocol handler base class"""
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'receivePacket') and
                callable(subclass.receivePacket) and
                hasattr(subclass, 'sendPacket') and
                callable(subclass.sendPacket) and
                hasattr(subclass, 'closeHandler') and
                callable(subclass.closeHandler) or
                NotImplemented)

    def __init__(self, ifMesh: MeshInterface) -> None:
        self.ifMesh: MeshInterface = ifMesh
        self.hdlrType: str | None = None
        self.isregistered: bool = False

    def __repr__(self):
        return f"{self.__class__.__name__}(Type: {self.hdlrType} isRegistered: {self.isRegistered})"

    @property
    def handlerType(self) -> str:
        return self.hdlrType

    @property
    def isRegistered(self) -> bool:
        return self.isRegistered


class NotImplementedHandler(ProtocolHandlerBase):
    """covers any unimplemented or unknown protocol as default.
    Typically, will ignore any messages, except during debug"""

    def __init__(self, ifMesh: MeshInterface, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = UNKNOWN_HANDLER

    def receivePacket(self, field: str, packet: Message) -> None:
        pass

    def sendPacket(self, data: dict) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass


@ProtocolHandlerBase.register
class DefaultHandler(ProtocolHandlerBase):
    """Protocol handler for all packets containing configuration data"""
    def __init__(self, ifMesh: MeshInterface, field2Topic, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = DEFAULT_HANDLER
        self.field2Topic = field2Topic

    def receivePacket(self, field: str, packet: Message) -> None:
        cfgData = google.protobuf.json_format.MessageToDict(packet, preserving_proto_field_name=True).get(field, None)
        if cfgData is not None:
            topicName = self.field2Topic[field]
            pub.sendMessage(topicName, field=field, data=cfgData)
        else:
            logger.debug(f"Pub-Sub: Received invalid message")

    def sendPacket(self, data: dict) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass

