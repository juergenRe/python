"""Handling of protocols: Interfacedefinition and ProtocolHandlerManager"""

import abc
import logging
from typing import Any, Callable
from pathlib import Path

from meshtastic.mesh_interface import MeshInterface
from meshtastic.protobuf import mesh_pb2

# Name definitions for protocol handler types
UNKNOWN_HANDLER = 'Unknown'
START_CONFIG_HANDLER = 'StartConfig'


class IProtocolHandler(metaclass=abc.ABCMeta):
    """The interface definition for MeshInterface"""
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'receivePacket') and
                callable(subclass.receivePacket) and
                hasattr(subclass, 'sendPacket') and
                callable(subclass.sendPacket) and
                hasattr(subclass, 'handlerType') and
                callable(subclass.handlerType) and
                hasattr(subclass, 'isRegistered') and
                callable(subclass.isRegistered) and
                hasattr(subclass, 'addHandler') and
                callable(subclass.addHandler) and
                hasattr(subclass, 'removeHandler') and
                callable(subclass.removeHandler) or
                NotImplemented)

    @abc.abstractmethod
    def receivePacket(self, packet) -> None:
        """Abstract method to receive a packet and decode it to a message"""
        raise NotImplementedError("Subclass must implement this method")

    @abc.abstractmethod
    def sendPacket(self, data: dict) -> Any:
        """Abstract method to send a message and encode it to a packet"""
        raise NotImplementedError("Subclass must implement this method")

    @property
    @abc.abstractmethod
    def handlerType(self) -> str:
        """Abstract method to add a protocol handler"""
        raise NotImplementedError("Subclass must implement this method")

    @property
    @abc.abstractmethod
    def isRegistered(self) -> bool:
        """Abstract property to return registration status"""
        raise NotImplementedError("Subclass must implement this method")

    @abc.abstractmethod
    def addHandler(self, address: str) -> Any:
        """Abstract method to remove a protocol handler"""
        raise NotImplementedError("Subclass must implement this method")

    @abc.abstractmethod
    def removeHandler(self, address: str) -> Any:
        """Abstract method to remove a protocol handler"""
        raise NotImplementedError("Subclass must implement this method")


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

