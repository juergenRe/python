"""Formal interface definition for MeshInterface"""

import abc
import logging
from typing import Any, Callable

from meshtastic.protobuf import mesh_pb2

class IRadioInterface(metaclass=abc.ABCMeta):
    """The interface definition for MeshInterface"""
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'connect') and
                callable(subclass.connect) and
                hasattr(subclass, 'close') and
                callable(subclass.close) and
                hasattr(subclass, 'sendToRadioImpl') and
                callable(subclass.sendToRadioImpl) and
                hasattr(subclass, 'receiveFromRadioImpl') and
                callable(subclass._receiveFromRadioImpl) or
                NotImplemented)

    @abc.abstractmethod
    def connect(self, address: str) -> Any:
        """Connect to the radio interface: open the connection to address"""
        raise NotImplementedError("Subclass must implement this method")

    @abc.abstractmethod
    def close(self) -> None:
        """Connect to the radio interface"""
        raise NotImplementedError("Subclass must implement this method")

    @abc.abstractmethod
    def sendToRadioImpl(self, toRadio: mesh_pb2.ToRadio) -> None:
        """Connect to the radio interface"""
        raise NotImplementedError("Subclass must implement this method")

    @abc.abstractmethod
    def _receiveFromRadioImpl(self) -> None:
        """Connect to the radio interface"""
        raise NotImplementedError("Subclass must implement this method")


class RadioInterfaceBase(IRadioInterface):
    def __init__(self, address: str, rcvCallback: Callable[[bytes], None], logCallback: Callable[[str], None]) -> None:
        self.address = address
        self._rcvCallback: Callable[[bytes], None] = rcvCallback
        self._logCallback: Callable[[str], None] = logCallback


logger = logging.getLogger(__name__)


class InterfaceOpenError(Exception):
    """An exception class for errors occurring during opening of the interface"""
    def __init__(self, message: str, ifType: str):
        super().__init__(message)
        self.ifType = ifType

    def __str__(self):
        msg = ''
        if len(self.args) > 0:
            msg = self.args[0]
        return f"Interface opening error from {self.ifType} - {msg}"


class SimulatedInterface(RadioInterfaceBase):
    def __init__(self, address: str, rcvCallback: Callable[[bytes], None], logCallback: Callable[[str], None]) -> None:
        super().__init__(address, rcvCallback, logCallback)

    def connect(self, address: str) -> None:
        logger.debug(f"Simulated Connect to: {address}")

    def close(self) -> None:
        logger.debug("Simulated Close")

    def sendToRadioImpl(self, toRadio: mesh_pb2.ToRadio) -> None:
        logger.debug(f"Simulated SendToRadio to: {toRadio}")

    def _receiveFromRadioImpl(self) -> None:
        logger.debug("Simulated ReceiveFromRadio")
