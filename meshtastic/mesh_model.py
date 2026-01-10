"""Keeps all data about the mesh entities during execution of a command.
Especially this is the local node and all discovered other nodes accessed via mesh"""
from copy import copy, deepcopy
from decimal import Decimal
from typing import Any
import logging

from meshtastic.protocol_base import formatFieldName
from meshtastic.util import convert_mac_addr


logger = logging.getLogger(__name__)


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
    nodeFields: list[str] = [formatFieldName(fn) for fn in
                             ['my_info', 'channel', 'config', 'moduleConfig', 'metadata', 'deviceUI']]

    def __init__(self, nodeNum: int, isLocal: bool = False) -> None:
        super().__init__(nodeNum)
        self._isLocal: bool = isLocal
        self._nodeInfo: NodeInfo | None = None
        self._data: dict = {}

    def __repr__(self) -> str:
        name = self._nodeInfo.getDataElement('user', 'long_name')
        if name is None:
            name = '???'
        locFlag = '<local>' if self._isLocal else ''
        return f"Node(0x{self._nodeNum:08x} {locFlag} {name})"

    def getName(self) -> tuple[str, str]:
        """return long and short name of this node. Return empty strings if not present"""
        longName: str = self._nodeInfo.getDataElement('user', 'long_name', '')
        shortName: str = self._nodeInfo.getDataElement('user', 'short_name', '')
        return longName, shortName

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

    def getLocalNode(self) -> Node | None:
        """returns the local node object"""
        return self.nodes.get(self._localNodeNum)

    def getLocalNodeNum(self) -> int:
        """returns the local node number"""
        return self._localNodeNum

    # def setLocalNodeNum(self, nodeNum: int) -> None:
    #     """sets the local node number"""
    #     self._localNodeNum = nodeNum
    #
    def getNode(self, nodeNum: int) -> Node | None:
        """returns the node with the given nodeNum"""
        return self.nodes.get(nodeNum)

    def getNodeIds(self) ->list[int]:
        """returns a list of all node numbers listed in the mesh"""
        return list(self.nodes.keys())

    def addNode(self, node: Node, overwrite: bool = True) -> None:
        """adds the node information to the dictionary of nodes.
        if overwrite is True: silently overwrite any existing entry. Otherwise raise an error"""
        if node.nodeNum in self.nodes and not overwrite:
            raise RuntimeError(f'Node {node} cannot added because it already exists in the mesh')
        self.nodes[node.nodeNum] = node
        if node.isLocal:
            self._localNodeNum = node.nodeNum
