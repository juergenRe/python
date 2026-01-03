"""Definitions for the commands which can be executed from meshtastic"""
import logging
from abc import abstractmethod
from collections import deque
from threading import Event
from typing import Any

from pubsub import pub  # type: ignore[import-untyped]
import topic_map

from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM

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
        self.answerEvent: Event = Event()

    @abstractmethod
    def execute(self, timeout: int) -> dict:
        """Executes the command"""
        raise NotImplementedError(f"Unknown Command")


class UnknownCommand(Command):
    """Placeholder for any unknown command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list):
        super().__init__(None, None, [])

    def execute(self, timeout: int):
        raise NotImplementedError(f"Unknown Command")


class GetConfigCommand(Command):
    """Triggers the reception of all the infos from local node"""
    def execute(self, timeout: int) -> dict:
        """execute the command"""
        pub.subscribe(self.onGetConfigFinished, topic_map.SUBS_STARTCOMM_FINISH)
        pub.subscribe(self.onReceiveData, topic_map.SUBS_STARTCOMM_RECEIVE)
        pub.sendMessage(topic_map.SUBS_STARTCOMM_START, timeout=timeout)
        if not self.answerEvent.wait(timeout):
            logger.debug("Connection to radio timed out. Stopping.")
            self.answerEvent.clear()
            return {'Error': 'Connection to radio timed out', 'Data': None}
        self.answerEvent.clear()
        return {'Error': None, 'Data': self.answerQueue.popleft()}

    def onGetConfigFinished(self, code: str):
        """callback when command is finished"""
        logger.debug(f"GetConfigFinished with code: {code}")
        self.answerEvent.set()

    def onReceiveData(self, field: str, data: dict):
        """callback for receiving data"""
        logger.debug(f"Received field {field} data: {data}")


class InfoCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, timeout: int):
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")


class SetCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, timeout: int):
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")