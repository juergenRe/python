"""Definitions for the commands which can be executed from meshtastic"""
import logging
from abc import abstractmethod
from collections import deque
from threading import Event
import json

from pubsub import pub  # type: ignore[import-untyped]
import topic_map

from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM
from meshtastic.mesh_model import MeshModel, Node, NodeInfo
from meshtastic.command_interface import ICommand, CmdError

logger = logging.getLogger(__name__)

class Command(ICommand):
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
    def execute(self, model: MeshModel, timeout: int) -> tuple:
        """Executes the command"""
        raise NotImplementedError(f"Unknown Command")


class UnknownCommand(Command):
    """Placeholder for any unknown command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list):
        super().__init__(None, None, [])

    def execute(self, model: MeshModel, timeout: int) -> tuple:
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
        if len(self.answerQueue) == 0:
            logger.debug(f"No data received")
            return CmdError.NO_DATA, "Error: No data received"

        try:
            # pop all data from queue and put it into an intermediate dict
            receivedData = {'node_info': [], 'channel': {}, 'config': {}, 'moduleConfig': {}}
            while len(self.answerQueue) > 0:
                field, newData = self.answerQueue.popleft()
                if field not in receivedData:
                    receivedData[field] = newData
                else:
                    data = receivedData[field]
                    if isinstance(data, list):
                        data.append(newData)
                    if isinstance(data, dict):
                        data.update(newData)

            # now transfer data to the mesh model. All data except the first node_info belong to the localNode
            # first find this node num from the data
            if 'my_info' not in receivedData:
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

    def formatAsJson(self, node: Node, field: str, prefix: str) -> str:
        """format an info entry"""
        data = node.getField(field)
        if data is None:
            s = ""
        else:
            s = f"{prefix}{json.dumps(data)}"
        return s

    def execute(self, model: MeshModel, timeout: int) -> tuple:
        """Show human-readable summary about this object"""
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        localNode: Node = model.getLocalNode()
        outList: list = []

        longName, shortName = localNode.getName()

        outList.append(f"Owner: {longName} ({shortName})")
        outList.append(self.formatAsJson(localNode, 'my_info', '\nMy info: '))
        outList.append(self.formatAsJson(localNode, 'metadata', '\nMetadata: '))
        outList.append("\n\nNodes in mesh: ")
        nodes = {}
        for node in model.nodes.values():
            # if macaddr := node.getDataElement('user', 'macaddr'):
            #     # decode the base64 value
            #     addr = convert_mac_addr(val)
            #     n2["user"]["macaddr"] = addr

            # use id as dictionary key for correct json format in list of nodes
            nodeid = node.getDataElement('user', 'id')
            if nodeid is not None:
                nodes[nodeid] = node
        outList.append(json.dumps(nodes, indent=2))

        infos = ''.join(outList)
        print(infos)
        return CmdError.OK, infos

        # prefs = ""
        # if self.localConfig:
        #     prefs = message_to_json(self.localConfig, multiline=True)
        # print(f"Preferences: {prefs}\n")
        # prefs = ""
        # if self.moduleConfig:
        #     prefs = message_to_json(self.moduleConfig, multiline=True)
        # print(f"Module preferences: {prefs}\n")
        # self.showChannels()
        #
        # return CmdError.OK, ""


class SetCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, model: MeshModel, timeout: int) -> tuple:
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        return CmdError.OK, ""
