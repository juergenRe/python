"""Keeps all data about the mesh entities during execution of a command.
Especially this is the local node and all discovered other nodes accessed via mesh"""
from copy import copy, deepcopy
from typing import Any


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
        return self._data.get(field)

    def getDataElement(self, field: str, key: str, default: Any = None) -> Any:
        """gets the data element with the given key from sub-dictionary (2nd level)"""
        data = self._data.get(field)
        if isinstance(data, dict):
            return data.get(key, default)
        return default

    # use deepcopy here to ensure we create new objects independent of previous threading context
    def setField(self, field: str, value: Any) -> None:
        """set value for a block of data"""
        self._data[field] = deepcopy(value)

    def setDataElement(self, field: str, key: str, value: Any) -> None:
        """Set data value in sub-dict"""
        if field in self._data:
            dx = self._data[field]
            if isinstance(dx, dict):
                dx[key] = deepcopy(value)


class NodeInfo(SubDict):
    """represents the content of node_info protobuf message
    externalized into this class to keep this data close together
    Node will _always_ contain one instance of this class"""
    def __init__(self, nodeInfo: dict) -> None:
        super().__init__(nodeInfo['num'])
        self._data = deepcopy(nodeInfo)


class Node(SubDict):
    """Node data structure and some helper methods"""

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
        shortName: str = self._nodeInfo.getDataElement('user', 'long_name', '')
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
        """create a node with the information given in a myInfo dictionary
        This case will always happen for the local node"""
        self._nodeNum = myInfo['my_node_num']
        self._data['my_info'] = deepcopy(myInfo)
        self._isLocal = True
        return self._nodeNum

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
