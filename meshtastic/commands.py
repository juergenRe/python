"""Definitions for the commands which can be executed from meshtastic"""
import logging
from collections import deque
from threading import Event
from typing import Any

from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM
from meshtastic.mesh_interface import MeshInterface

logger = logging.getLogger(__name__)


class Command:
    """Base class for all command implementations"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list):
        self.destinationNode: None | int = destNode
        if destNode is None:
            self.destNode = BROADCAST_ADDR
        self.transactionId: None | int = None
        self.cmdName: str = self.__class__.__name__
        self.parameter: list = parameter
        self.answerQueue: deque = deque()
        self.answerEvent: Event = Event()

    def execute(self, ifce: MeshInterface, timeout: int) -> dict:
        """Executes the command"""
        raise NotImplementedError(f"Unknown Command")

    def completionCallback(self, cmd: str, data: Any) -> None:
        """Callback function that is called when the command is completed.
        This callback is executed in the receiving thread context"""
        self.answerQueue.append((cmd, data))
        self.answerEvent.set()

class UnknownCommand(Command):
    """Placeholder for any unknown command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list):
        super().__init__(None, None, [])

    def execute(self, ifce: MeshInterface, timeout: int):
        raise NotImplementedError(f"Unknown Command")


class GetConfigCommand(Command):
    """Triggers the reception of all the infos from local node"""
    def execute(self, ifce: MeshInterface, timeout: int) -> dict:
        ifce.connectAndGetConfig('getConfig', self.completionCallback)
        if not self.answerEvent.wait(timeout):
            logger.debug("Connection to radio timed out. Stopping.")
            self.answerEvent.clear()
            return {'Error': 'Connection to radio timed out', 'Data': None}
        self.answerEvent.clear()
        return {'Error': None, 'Data': self.answerQueue.popleft()}


class InfoCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, ifce: MeshInterface, timeout: int):
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")


class SetCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, ifce: MeshInterface, timeout: int):
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")