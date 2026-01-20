"""Definitions for the commands which can be executed from meshtastic"""
import logging
from abc import abstractmethod
from collections import deque
from random import randint
from threading import Event

from pubsub import pub  # type: ignore[import-untyped]

from meshtastic import topic_map, stripnl
from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM
from meshtastic.mesh_model import MeshModel, Node, NodeInfo
from meshtastic.command_interface import ICommand, CmdError
from meshtastic.protobuf import portnums_pb2
from meshtastic.protocol_base import formatFieldName
from meshtastic.topic_map import SUBS_PACKET_REQ, SUBS_PACKET_PUB

logger = logging.getLogger(__name__)


class Command(ICommand):
    """
    Base class for all command implementations
    destNode: destination node as input argument ("!??????" | "^all" | "^local" | "0x????")
    """
    currentPacketId: int = 0        # have one common packetId for all Commands used

    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list):
        if destNode is None:
            self.destNode = BROADCAST_ADDR
        self.destinationNode: str = destNode
        self.chIndex: int | None = chIndex
        self.transactionId: None | int = None
        self.cmdName: str = self.__class__.__name__
        self.parameter: list = parameter
        self.answerEvent: Event = Event()
        self.answerQueue: deque = deque()
        self.outputMsg = ''

    def _getPacketId(self) -> int:
        """generates the next packet ID and returns it"""
        nextPacketId = (Command.currentPacketId + 1) & 0xFFFFFFFF
        nextPacketId = nextPacketId & 0x3FF  # == (0xFFFFFFFF >> 22), masks upper 22 bits
        randomPart = (randint(0, 0x3FFFFF) << 10) & 0xFFFFFFFF  # generate number with 10 zeros at end
        Command.currentPacketId = nextPacketId | randomPart  # combine
        return Command.currentPacketId

    @abstractmethod
    def execute(self, model: MeshModel, timeout: int, **kwargs) -> tuple:
        """Executes the command"""
        raise NotImplementedError(f"Unknown Command")

class UnknownCommand(Command):
    """Placeholder for any unknown command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list):
        super().__init__(None, None, [])

    def execute(self, model: MeshModel, timeout: int, **kwargs) -> tuple:
        raise NotImplementedError(f"Unknown Command")


FIELD_MYINFO = formatFieldName('my_info')
FIELD_NODEINFO = formatFieldName('node_info')

class GetConfigCommand(Command):
    """Triggers the reception of all the infos from local node"""
    def execute(self, model: MeshModel, timeout: int, **kwargs) -> tuple:
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


class SetCommand(Command):
    """Defines the set command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, model: MeshModel, timeout: int, **kwargs) -> tuple:
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        return CmdError.OK, ""

class GetCannedMessageCommand(Command):
    """Defines the command for retrieving the canned message from radio"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)
        self.outputMsg = 'canned_plugin_message:{value}'

    def execute(self, model: MeshModel, timeout: int, **kwargs) -> tuple:
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        result = 'None'
        node = model.localNode
        if self.destinationNode != '^all':
            node = model.getNodeFromStr(self.destinationNode)
        if (cm := node.getDataElement('other', 'cannedMessage')) is not None:
            result = CmdError.OK, self.outputMsg.format(value=cm)
            return result
        try:
            pub.subscribe(self.onReceiveData, topic_map.SUBS_PACKET_PUB)
            d = {"get_canned_message_module_messages_request": True}
            if pk := node.sessionKey is not None:
                d['session_passkey'] = pk

            self._sendAdmin(model, node.nodeNum, d)

            if not self.answerEvent.wait(timeout):
                logger.debug("Connection to radio timed out. Stopping.")
                self.answerEvent.clear()
                result = CmdError.TIMEOUT, "Error: Connection to radio timed out"
                return result
            self.answerEvent.clear()
            if len(self.answerQueue) == 0:
                logger.debug(f"No data received")
                result = CmdError.NO_DATA, "Error: No data received"
                return result

            try:
                data = self.answerQueue.popleft()
                payload = data[1]['decoded']['admin']
                cm = ''
                if 'sessionPasskey' in payload:
                    node.sessionKey = payload['sessionPasskey']
                if 'getCannedMessageModuleMessagesResponse' in payload:
                    cm = payload['getCannedMessageModuleMessagesResponse']
                    node.setDataElement('other', 'cannedMessage', cm)
                result = CmdError.OK, self.outputMsg.format(value=cm)
                return result
            except Exception as ex:
                logger.debug(f"Exception: {ex}")
                result = CmdError.ERROR, "Error: Exception in execution"
                return result
        finally:
            pub.unsubscribe(self.onReceiveData, topic_map.SUBS_PACKET_PUB)
            logger.debug(f"Command {self.cmdName} finished with code: {result}")

    def onReceiveData(self, field: str, data: dict, requestId: int):
        """subscription callback, pick only data elements as response to sent message"""
        # requestId = 0
        logger.debug(f"Received message {field} data: {stripnl(data)} ID: {requestId}")
        if requestId is not None and requestId == self.transactionId:
            self.answerQueue.append((requestId, data), )
            self.answerEvent.set()

    def _sendAdmin(self,
                   model: MeshModel,
                   nodeNum: int,
                   data: dict,
                   wantResponse: bool = True,
                   adminIndex: int = 0):
        """Send an admin message to the specified node (or the local node if destNodeNum is zero)"""

        if adminIndex == 0:  # unless a special channel index was used, we want to use the admin index
            adminIndex = model.localNode.getAdminChannelIndex()
        logger.debug(f"adminIndex:{adminIndex}")
        self.transactionId = self._getPacketId()
        hopLimit = model.getNode(nodeNum).getDataElement('lora', 'hop_limit', None)
        if hopLimit is None:
            hopLimit = model.getNode(model.localNodeNum).getDataElement('lora', 'hop_limit', 3)

        args = {
            'packetId': self.transactionId,
            'destinationId': nodeNum,
            'channelIndex': adminIndex,
            'hopLimit': hopLimit,
            'portNum': portnums_pb2.PortNum.ADMIN_APP,
            'wantAck': True,
            'wantResponse': wantResponse,
            'pkiEncrypted': True
        }
        pub.sendMessage(SUBS_PACKET_REQ, data=data, args=args)
