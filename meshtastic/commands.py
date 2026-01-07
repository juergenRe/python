"""Definitions for the commands which can be executed from meshtastic"""
import enum
import logging
from abc import abstractmethod
from collections import deque
from threading import Event
from enum import Enum

from pubsub import pub  # type: ignore[import-untyped]
import topic_map

from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM
from meshtastic.mesh_model import MeshModel, Node, NodeInfo

logger = logging.getLogger(__name__)

class CmdError(enum.Enum):
    """Enumerations for command return/error values"""
    OK = 0
    ERROR = 1   # generic error without further spec
    TIMEOUT = 2
    NO_DATA = 3
    INCOMPLETE_DATA = 4



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
        self.answerQueue: deque = deque()

    @abstractmethod
    def execute(self, model: MeshModel, timeout: int) -> dict:
        """Executes the command"""
        raise NotImplementedError(f"Unknown Command")


class UnknownCommand(Command):
    """Placeholder for any unknown command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list):
        super().__init__(None, None, [])

    def execute(self, model: MeshModel, timeout: int):
        raise NotImplementedError(f"Unknown Command")


class GetConfigCommand(Command):
    """Triggers the reception of all the infos from local node"""
    def execute(self, model: MeshModel, timeout: int) -> tuple:
        """execute the command"""
        pub.subscribe(self.onGetConfigFinished, topic_map.SUBS_STARTCOMM_FINISH)
        pub.subscribe(self.onReceiveData, topic_map.SUBS_STARTCOMM_RECEIVE)
        pub.subscribe(self.onReceiveData, topic_map.SUBS_MY_INFO_PUB)
        pub.subscribe(self.onReceiveData, topic_map.SUBS_METADATA_PUB)
        pub.subscribe(self.onReceiveData, topic_map.SUBS_CHANNEL_PUB)
        pub.subscribe(self.onReceiveData, topic_map.SUBS_CONFIG_PUB)
        pub.subscribe(self.onReceiveData, topic_map.SUBS_NODEINFO_PUB)
        pub.sendMessage(topic_map.SUBS_STARTCOMM_START, timeout=timeout)
        receivedData: dict = {}

        if not self.answerEvent.wait(timeout):
            logger.debug("Connection to radio timed out. Stopping.")
            self.answerEvent.clear()
            return CmdError.TIMEOUT, "Connection to radio timed out"
        self.answerEvent.clear()
        if len (self.answerQueue) == 0:
            logger.debug(f"No data received")
            return CmdError.NO_DATA, "Error: No data received"

        try:
            # pop all data from queue and put it into an intermediate dict
            receivedData = {'node_info': [], 'channel': {}, 'config': {}, 'moduleConfig': {}}
            while len (self.answerQueue) > 0:
                field, newData = self.answerQueue.popleft()
                if field not in receivedData:
                    receivedData[field] = newData
                else:
                    data = receivedData[field]
                    if isinstance(data, list):
                        data.append(newData)
                    if isinstance(data, dict):
                        data.update(newData)

            # now transfer data to the mesh model. All data except the first node_info belong to the localnode
            # first find this node num from the data
            if not 'my_info' in receivedData:
                logger.debug(f"No 'my_info' data received. Data incomplete, cannot proceed.")
                return CmdError.INCOMPLETE_DATA, "Error: No 'my_info' data received. Data incomplete, cannot proceed."
            else:
                localNode: Node = Node(0, True)
                localNode.createFromMyInfo(receivedData['my_info'])

            for field, value in receivedData.items():
                if field == 'my_info' or field == 'node_info':
                    continue
                localNode.setField(field, value)

            # Now treat the node_info data. One of them belongs to the local node
            niList = [NodeInfo(info) for info in receivedData['node_info']]
            for ni in niList:
                if ni.nodeNum == localNode.nodeNum:
                    nd = localNode
                else:
                    nd = Node(ni.nodeNum)
                nd.createFromNodeInfo(ni)
                model.addNode(nd)
            return CmdError.OK, ''
        except Exception as ex:
            logger.debug(f"Exception: {ex}")
            return CmdError.ERROR, "Exception in execution"
        finally:
            if receivedData:
                del receivedData
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_STARTCOMM_FINISH)
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_STARTCOMM_RECEIVE)
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_MY_INFO_PUB)
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_METADATA_PUB)
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_CHANNEL_PUB)
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_CONFIG_PUB)
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_NODEINFO_PUB)

    def onGetConfigFinished(self, code: str):
        """callback when command is finished"""
        logger.debug(f"GetConfigFinished with code: {code}")
        self.answerEvent.set()

    def onReceiveData(self, field: str, data: dict):
        """callback for receiving data from various sources"""
        logger.debug(f"Received field {field} data: {data}")
        self.answerQueue.append((field, data), )


class InfoCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, model: MeshModel, timeout: int):
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")


class SetCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, model: MeshModel, timeout: int):
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")