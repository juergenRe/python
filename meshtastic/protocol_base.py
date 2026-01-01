"""Handling of protocols: Interfacedefinition and ProtocolHandlerManager"""

import abc
import logging
from typing import Any, Callable
from pathlib import Path

from meshtastic.protocol_interface import IProtocolHandler
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protobuf import mesh_pb2

# Name definitions for protocol handler types
UNKNOWN_HANDLER = 'Unknown'
START_CONFIG_HANDLER = 'StartConfig'


class ProtocolHandlerBase(IProtocolHandler):
    """The protocol handler base class"""
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'receivePacket') and
                callable(subclass.receivePacket) and
                hasattr(subclass, 'sendPacket') and
                callable(subclass.sendPacket) and
                hasattr(subclass, 'addHandler') and
                callable(subclass.addHandler) and
                hasattr(subclass, 'removeHandler') and
                callable(subclass.removeHandler) or
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

    def __init__(self, ifMesh: MeshInterface) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = UNKNOWN_HANDLER

    def receivePacket(self, packet) -> None:
        pass

    def sendPacket(self, data: dict) -> Any:
        pass

    def addHandler(self, address: str) -> Any:
        pass

    def removeHandler(self, address: str) -> Any:
        pass

