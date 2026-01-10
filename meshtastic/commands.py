"""Definitions for the commands which can be executed from meshtastic"""
import base64
import logging
from abc import abstractmethod
from collections import deque
from threading import Event
import json
from typing import Any

from google.protobuf.message import Message
from pubsub import pub  # type: ignore[import-untyped]

from google.protobuf.json_format import ParseDict

from meshtastic.protobuf import apponly_pb2

from meshtastic import topic_map
from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM
from meshtastic.mesh_model import MeshModel, Node, NodeInfo
from meshtastic.command_interface import ICommand, CmdError
from meshtastic.util import pskToString
from meshtastic.protocol_base import formatFieldName
from meshtastic.protocol_channel_data import ROLE_PRIMARY, ROLE_SECONDARY, ROLE_DISABLED, ROLE_NONE

logger = logging.getLogger(__name__)


URL_PREFIX = 'https://meshtastic.org/e/#'

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

FIELD_MYINFO = formatFieldName('my_info')
FIELD_NODEINFO = formatFieldName('node_info')

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
            receivedData = {FIELD_NODEINFO: [], 'channel': {}, 'config': {}, 'moduleConfig': {}}
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
            if FIELD_MYINFO not in receivedData:
                logger.debug(f"No '{FIELD_MYINFO}' data received. Data incomplete, cannot proceed.")
                return CmdError.INCOMPLETE_DATA, f"Error: No '{FIELD_MYINFO}' data received. Data incomplete, cannot proceed."
            else:
                localNode: Node = Node(0, True)
                localNode.createFromMyInfo(receivedData[FIELD_MYINFO])

            for field, value in receivedData.items():
                if field == FIELD_MYINFO or field == FIELD_NODEINFO:
                    continue
                localNode.setField(field, value)

            # Now treat the node_info data. One of them belongs to the local node
            niList = [NodeInfo(info) for info in receivedData[FIELD_NODEINFO]]
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

    def formatAsJson(self, node: Node, field: str, prefix: str, indent=None) -> str:
        """format an info entry"""
        data = node.getField(field)
        if data is None:
            s = ""
        else:
            s = f"{prefix}{json.dumps(data, indent=indent)}"
        return s

    def formatChannels(self, node: Node, field: str, prefix: str) -> str:
        """format channel info"""
        chanData = node.getField(field)
        if chanData is None:
            s = ""
        else:
            cl = []
            for idx, value in chanData.items():
                if value['role'] != ROLE_NONE and value['role'] != ROLE_DISABLED:
                    pskBytes = base64.b64decode(value['settings'].get('psk').encode('utf-8'))
                    pskLit = pskToString(pskBytes)
                    cs = f"  Index {idx}: {value['role']} psk={pskLit} {json.dumps(value['settings'])}"
                    cl.append(cs)
            publicURL = self.getUrl(node, includeAll=False)
            adminURL = self.getUrl(node, includeAll=True)
            cl.append(f"\nPrimary channel URL: {publicURL}")
            if adminURL != publicURL:
                cl.append(f"Complete URL (includes all channels): {adminURL}")
            s = f"{prefix}{'\n'.join(cl)}"
        return s

    def formatNodeInfoAsJson(self, model: MeshModel, prefix: str, indent=None) -> str:
        """Format node info as json"""
        def infoJson(obj) -> dict:
            """JSON encoder for NodeInfo objects"""
            if isinstance(obj, Node):
                return obj.formatInfoForJson()
            raise TypeError(f'Cannot serialize object of {type(obj)}')

        nodes = {}
        for node in model.nodes.values():
            # use id as dictionary key for correct JSON format in list of nodes
            nodeid = node.getDataElement('user', 'id')
            if nodeid is not None:
                nodes[nodeid] = node
        return f"\n{prefix} {json.dumps(nodes, indent=2, default=infoJson)}"

    def getUrl(self, node: Node, includeAll: bool = True) -> str:
        """The sharable URL that describes the current channel"""
        # Only keep the primary/secondary channels, assume primary is first
        chanData = node.getField('channel')
        if chanData is not None:
            chanList = [chan['settings']
                        for chan in chanData.values()
                        if chan['role'] == ROLE_PRIMARY or (includeAll and chan['role'] == ROLE_SECONDARY)]

            if node.getField('config') is None:
                logger.debug(f"config for node {node.nodeNum} is missing. Aborting operation")
                # self.requestConfig(self.localConfig.DESCRIPTOR.fields_by_name.get('lora'))
            loraCfg = node.getDataElement('config', 'lora')
            chanSetDict: dict[str, Any] = {'settings': chanList, 'lora_config': loraCfg}

            # fill channelSet message
            channelSet = apponly_pb2.ChannelSet()
            ParseDict(chanSetDict, channelSet)
            some_bytes = channelSet.SerializeToString()
            s = base64.urlsafe_b64encode(some_bytes).decode("ascii")
            s = s.replace("=", "").replace("+", "-").replace("/", "_")
            return f"{URL_PREFIX}{s}"
        else:
            return ''

    def execute(self, model: MeshModel, timeout: int) -> tuple:
        """Show human-readable summary about this object"""
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        if self.destinationNode == BROADCAST_ADDR:
            localNode: Node = model.getLocalNode()
            outList: list = []

            longName, shortName = localNode.getName()

            outList.append(f"Owner: {longName} ({shortName})")
            outList.append(self.formatAsJson(localNode, FIELD_MYINFO, '\nMy info: '))
            outList.append(self.formatAsJson(localNode, 'metadata', '\nMetadata: '))
            outList.append(self.formatNodeInfoAsJson(model, prefix='\n\nNodes in mesh: ', indent=2))
            outList.append(self.formatAsJson(localNode, 'config', '\nPreferences: ', indent=2))
            outList.append(self.formatAsJson(localNode, 'moduleConfig', '\nModule preferences: ', indent=2))
            outList.append(self.formatChannels(localNode, 'channel', '\nChannels:\n'))

            infos = ''.join(outList)
            print(infos)
            return CmdError.OK, infos
        else:
            return CmdError.ERROR, ("Showing info of remote node is not supported.\n"
                                    "Use the '--get' command for a specific configuration (e.g. 'lora') instead.")

class SetCommand(Command):
    """Defines the info command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, model: MeshModel, timeout: int) -> tuple:
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        return CmdError.OK, ""
