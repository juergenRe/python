"""Keeps all data about the mesh entities during execution of a command.
Especially this is the local node and all discovered other nodes accessed via mesh"""
import datetime as dt
from copy import deepcopy
from decimal import Decimal
from typing import Any
import logging
import base64

from google.protobuf.json_format import ParseDict

from meshtastic import BROADCAST_NUM
from meshtastic.protobuf import apponly_pb2
from meshtastic.protocol_base import formatFieldName
from meshtastic.util import convert_mac_addr

logger = logging.getLogger(__name__)

URL_PREFIX = 'https://meshtastic.org/e/#'

ROLE_PRIMARY = 'PRIMARY'
ROLE_SECONDARY = 'SECONDARY'
ROLE_DISABLED = 'DISABLED'
ROLE_NONE = 'NONE'


class SubDict:
    """Represents an object which carries a nested dictionary structure of 2 levels
    data = {'key1': value, 'key2: {'field': data} }
    In addition the structure carries a unique nodeNum
    """

    def __init__(self, nodeNum: int) -> None:
        self._nodeNum: int = nodeNum
        self._data: dict[str, Any] = {'user': {}}  # initialize with empty user

    @property
    def nodeNum(self) -> int:
        """return integer node number"""
        return self._nodeNum

    def getField(self, field: str) -> Any:
        """returns the value for the given field (1st level of dict)"""
        return self._data.get(formatFieldName(field))

    def getDataElement(self, field: str, key: str, default: Any = None) -> Any:
        """gets the data element with the given key from sub-dictionary (2nd level)"""
        data = self._data.get(formatFieldName(field))
        if isinstance(data, dict):
            return data.get(formatFieldName(key), default)
        return default

    # use deepcopy here to ensure we create new objects independent of previous threading context
    def setField(self, field: str, value: Any) -> None:
        """set value for a block of data"""
        self._data[formatFieldName(field)] = deepcopy(value)

    def setDataElement(self, field: str, key: str, value: Any) -> None:
        """Set data value in sub-dict"""
        f = formatFieldName(field)
        if f in self._data:
            dx = self._data[f]
            if isinstance(dx, dict):
                dx[formatFieldName(key)] = deepcopy(value)


class NodeInfo(SubDict):
    """represents the content of node_info protobuf message
    externalized into this class to keep this data close together
    Node will _always_ contain one instance of this class"""

    def __init__(self, nodeInfo: dict) -> None:
        super().__init__(nodeInfo['num'])
        self._data = deepcopy(nodeInfo)
        self._fixPosition()

    def _fixPosition(self) -> None:
        """Add float values for position if integer values are present"""
        try:
            position = self._data['position']

            if "latitudeI" in position:
                position["latitude"] = float(position["latitudeI"] * Decimal("1e-7"))
            if "longitudeI" in position:
                position["longitude"] = float(position["longitudeI"] * Decimal("1e-7"))
        except (KeyError, TypeError):
            logger.debug(f"{NodeInfo} has no position field")


class Node(SubDict):
    """Node data structure and some helper methods
    NodeInfo data will be mapped directly to the level of the Node itself:
    external requesters don't need (and don't have) to know internal structure
    """

    # defines what substructure lies behind the field names. Used to initialize internal data
    nodeFieldsDef: dict[str, Any] = {formatFieldName(tp[0]): tp[1] for tp in
                                     [('my_info', {}),
                                      ('channel', []),
                                      ('config', {}),
                                      ('moduleConfig', {}),
                                      ('metadata', {}),
                                      ('deviceUI', {}),
                                      ('other', {})
                                      ]}

    # nodeFields are those fields to be stored within the node data structure.
    # All other fields will be stored within nodeInfo data
    nodeFields: list[str] = list(nodeFieldsDef.keys())
    sessionKeyTimeoutVal = dt.timedelta(seconds=300)
    minFWVersion = '2.7'

    @classmethod
    def toNodeNum(cls, nodeStr: str, numLocal: int) -> int:
        """convert string to node number"""
        if nodeStr == '^all':
            return BROADCAST_NUM
        if nodeStr == '^local':
            return numLocal
        s = nodeStr.strip().lower()
        try:
            if s.startswith('!'):
                nodeNum = int(s[1:], 16)
            elif s.startswith('0x') == 0:
                nodeNum = int(s, 16)
            else:
                nodeNum = numLocal
        except ValueError:
            nodeNum = numLocal
        return nodeNum

    def __init__(self, nodeNum: int, isLocal: bool = False) -> None:
        super().__init__(nodeNum)
        self._isLocal: bool = isLocal
        self._nodeInfo: NodeInfo | None = None
        self._data: dict = deepcopy(self.nodeFieldsDef)
        self._sessionKey: str | None = None
        self._sessionKeyTimeout: dt.datetime = dt.datetime(1, 1, 1)

    def __repr__(self) -> str:
        name = self._nodeInfo.getDataElement('user', 'long_name')
        if name is None:
            name = '???'
        locFlag = '<local>' if self._isLocal else ''
        return f"Node(0x{self._nodeNum:08x} {locFlag} {name})"

    def getName(self, default='') -> tuple[str, str]:
        """return long and short name of this node. Return empty strings if not present"""
        longName: str = self._nodeInfo.getDataElement('user', 'long_name', default=default)
        shortName: str = self._nodeInfo.getDataElement('user', 'short_name', default=default)
        return longName, shortName

    @property
    def isValidFW(self) -> bool | None:
        """Checks if actual fw of a node is acceptable to be communicated with"""
        fwVersion: str = self.getDataElement('metadata', 'firmware_version')
        if fwVersion is None:
            return None
        else:
            fwact = fwVersion.strip().split('.')
            fwmin = self.minFWVersion.split('.')
            cnt = min(len(fwact), len(fwmin))
            for i in range(cnt):
                if fwact[i] < fwmin[i]:
                    return False
            return True

    @property
    def sessionKey(self) -> str | None:
        """return session key of this node if present and not timed out"""
        if dt.datetime.now() > self._sessionKeyTimeout:
            self._sessionKey = None
            self._sessionKeyTimeout: dt.datetime = dt.datetime(1, 1, 1)
        return self._sessionKey

    @sessionKey.setter
    def sessionKey(self, value: str):
        self._sessionKey = value
        self._sessionKeyTimeout = dt.datetime.now() + self.sessionKeyTimeoutVal

    def getAdminChannelIndex(self):
        """return admin channel index if present"""
        # Fixme: add real implementation
        return 0

    def getUrl(self, includeAll: bool = True) -> str:
        """The sharable URL that describes the current channel"""
        # Only keep the primary/secondary channels, assume primary is first
        chanData = self.getField('channel')
        if chanData is not None:
            chanList = [chan['settings']
                        for chan in chanData.values()
                        if chan['role'] == ROLE_PRIMARY or (includeAll and chan['role'] == ROLE_SECONDARY)]

            if self.getField('config') is None:
                logger.debug(f"config for node {self.nodeNum} is missing. Aborting operation")
                # self.requestConfig(self.localConfig.DESCRIPTOR.fields_by_name.get('lora'))
            loraCfg = self.getDataElement('config', 'lora')
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

    @property
    def isLocal(self) -> bool:
        """Getter for _isLocal"""
        return self._isLocal

    def createFromNodeInfo(self, nodeInfo: NodeInfo) -> None:
        """create a node with the information given in a nodeInfo object"""
        self._nodeNum = nodeInfo._nodeNum
        self._nodeInfo = nodeInfo
        return None

    def createFromMyInfo(self, myInfo: dict) -> int:
        """
        create a node with the information given in a myInfo dictionary
        Take care to use methods defined in superclass to handle correctly field/key names!
        This case will always happen for the local node
        """
        self.setField('my_info', myInfo)
        self._nodeNum = self.getDataElement('my_info', 'my_node_num')
        self._isLocal = True
        return self._nodeNum

    # overriding methods of superclass for extended use
    def getField(self, field: str) -> Any:
        """returns the value for the given field (1st level of dict)"""
        if formatFieldName(field) in self.nodeFields:
            return super().getField(field)
        else:
            return self._nodeInfo.getField(field)

    def getDataElement(self, field: str, key: str, default: Any = None) -> Any:
        """gets the data element with the given key from sub-dictionary (2nd level)"""
        if formatFieldName(field) in self.nodeFields:
            return super().getDataElement(field, key, default)
        else:
            return self._nodeInfo.getDataElement(field, key, default)

    # use deepcopy here to ensure we create new objects independent of previous threading context
    def setField(self, field: str, value: Any) -> None:
        """set value for a block of data. Need to know where to store: in Node or NodeInfo"""
        if formatFieldName(field) in self.nodeFields:
            super().setField(field, value)
        else:
            self._nodeInfo.setField(field, value)

    def setDataElement(self, field: str, key: str, value: Any) -> None:
        """Set data value in sub-dict"""
        if formatFieldName(field) in self.nodeFields:
            super().setDataElement(field, key, value)
        else:
            self._nodeInfo.setDataElement(field, key, value)

    def formatInfoForJson(self) -> dict:
        """returns the NodeInfo as JSON serializable representation of the node"""
        dni = deepcopy(self._nodeInfo._data)
        if 'macaddr' in dni['user']:
            ma = convert_mac_addr(dni['user']['macaddr'])
            dni['user']['macaddr'] = ma
        return dni


class MeshModel:
    """Keeps all data about the mesh entities during execution of a command."""

    def __init__(self) -> None:
        self.nodes: dict[int, Node] = {}
        self._localNodeNum: int = -1

    @property
    def localNode(self) -> Node | None:
        """returns the local node object"""
        return self.nodes.get(self._localNodeNum)

    @property
    def localNodeNum(self) -> int:
        """returns the local node number"""
        return self._localNodeNum

    # def setLocalNodeNum(self, nodeNum: int) -> None:
    #     """sets the local node number"""
    #     self._localNodeNum = nodeNum
    #
    def getNode(self, nodeNum: int) -> Node | None:
        """returns the node with the given nodeNum"""
        return self.nodes.get(nodeNum)

    def getNodeFromStr(self, nodeStr: str) -> Node | None:
        """returns the node from nodeID as string"""
        return self.getNode(Node.toNodeNum(nodeStr, self._localNodeNum))

    def getNodeIds(self) -> list[int]:
        """returns a list of all node numbers listed in the mesh"""
        return list(self.nodes.keys())

    def addNode(self, node: Node, overwrite: bool = True) -> None:
        """adds the node information to the dictionary of nodes.
        if overwrite is True: silently overwrite any existing entry, otherwise raise an error"""
        if node.nodeNum in self.nodes and not overwrite:
            raise RuntimeError(f'Node {node} cannot added because it already exists in the mesh')
        self.nodes[node.nodeNum] = node
        if node.isLocal:
            self._localNodeNum = node.nodeNum
