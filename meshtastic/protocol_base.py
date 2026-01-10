"""Handling of protocols: Interfacedefinition and ProtocolHandlerManager"""

import abc
import logging
from typing import Any, Callable
from pathlib import Path

from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson, MessageToDict

from pubsub import pub  # type: ignore[import-untyped]

from meshtastic.util import stripnl, snake_to_camel
from meshtastic.protocol_interface import IProtocolHandler
from meshtastic.mesh_interface import MeshInterface

logger = logging.getLogger(__name__)

# Name definitions for protocol handler types
UNKNOWN_HANDLER = 'Unknown'
DEFAULT_HANDLER = 'Default'
START_CONFIG_HANDLER = 'StartConfig'
LOGGING_HANDLER = 'Logging'
CHANNEL_HANDLER = 'ChannelData'
CONFIG_HANDLER = 'ConfigHandler'

# Define settings for serialization of messages
PRESERVE_FIELDNAME = False          # False will convert snake_case to lowerCamelCase
ALL_FIELDS = True                   # True: will return all defined fields with default values if not present in the message


def formatFieldName(fieldName: str) -> str:
    """
    return either snake or camel case field name according to setting PRESERVE_FIELDNAME
    This returned field name will then be used within the model data
    Attention: Canonical form is snake_case as define din the proto files!
    """
    return snake_to_camel(fieldName) if not PRESERVE_FIELDNAME else fieldName


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

    def messageToJson(self, message: Message, multiline: bool = False) -> str:
        """Return protobuf message as JSON. Always print all fields, even when not present in data.
        Take care about changed interface def of protobuf"""
        try:
            json = MessageToJson(message, preserving_proto_field_name = PRESERVE_FIELDNAME, always_print_fields_with_no_presence=ALL_FIELDS)
        except TypeError:
            json = MessageToJson(message, including_default_value_fields=ALL_FIELDS) # type: ignore[call-arg] # pylint: disable=E1123
        return stripnl(json) if not multiline else json

    def messageToDict(self, message: Message, allFields: bool = ALL_FIELDS) -> dict:
        """Return protobuf message as dict. Always print all fields, even when not present in data.
        Take care about changed interface def of protobuf"""
        try:
            return MessageToDict(message, preserving_proto_field_name = PRESERVE_FIELDNAME, always_print_fields_with_no_presence=allFields)
        except TypeError:
            return MessageToDict(message, including_default_value_fields=ALL_FIELDS) # type: ignore[call-arg] # pylint: disable=E1123


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
        fieldT = formatFieldName(field)
        allFields = False if field == 'node_info' else ALL_FIELDS
        cfgData = self.messageToDict(packet, allFields).get(fieldT, None)
        if cfgData is not None:
            topicName = self.field2Topic[field]
            pub.sendMessage(topicName, field=fieldT, data=cfgData)
        else:
            logger.debug(f"Pub-Sub: Received invalid message")

    def sendPacket(self, data: dict) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass

