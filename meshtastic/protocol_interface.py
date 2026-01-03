"""The interface definition for all protocol handlers"""
import abc
from typing import Any, Callable

from google.protobuf.message import Message


class IProtocolHandler(metaclass=abc.ABCMeta):
    """The interface definition for all protocol handlers"""
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
                hasattr(subclass, 'closeHandler') and
                callable(subclass.closeHandler) or
                NotImplemented)

    @abc.abstractmethod
    def receivePacket(self, field: str, packet: Message) -> None:
        """Abstract method to receive a packet from underlying level and decode it to a message"""
        raise NotImplementedError("Subclass must implement this method")

    @abc.abstractmethod
    def sendPacket(self, data: dict) -> Any:
        """Abstract method to receive data from the command, encode it to a packet and
        transfer it using the underlying level"""
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
    def closeHandler(self) -> Any:
        """Abstract method to shut down a protocol handler"""
        raise NotImplementedError("Subclass must implement this method")


