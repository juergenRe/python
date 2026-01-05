"""Keeps all data about the mesh entities during execution of a command.
Especially this is the local node and all discovered other nodes accessed via mesh"""
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
    def getNodeNum(self) -> int:
        return self._nodeNum

    def getData(self, key: str) -> Any:
        """returns the value for the given key"""
        return self._data.get(key)

    def getField(self, key: str, field: str) -> Any:
        """gets the field with the given key from sub-dictionary"""
        data = self._data.get(key)
        if isinstance(data, dict):
            return data.get(field)
        return None

    def setData(self, key: str, value: Any) -> None:
        """set value for a block of data"""
        if key in self._data:
            self._data[key] = value

    def setField(self, key: str, field: str, value: Any) -> None:
        """Set data value in sub-dict"""
        if key in self._data:
            dx = self._data[key]
            if isinstance(dx, dict):
                dx[field] = value


class NodeInfo(SubDict):
    """represents the content of node_info protobuf message
    externalized into this class to keep this data close together
    Node will _always_ contain one instance of this class"""
    def __init__(self, nodeNum: int) -> None:
        super().__init__(nodeNum)


class Node(SubDict):
    """Node data structure and some helper methods"""

    def __init__(self, nodeNum: int, isLocal: bool) -> None:
        super().__init__(nodeNum)
        self._isLocal: bool = isLocal
        self._nodeInfo: NodeInfo | None = None
        self._data: dict = {}

    def __repr__(self) -> str:
        name = self._nodeInfo.getField('user', 'long_name')
        if name is None:
            name = '???'
        return f"Node(0x{self._nodeNum:08x} {name})"

    def createFromNodeInfo(self, nodeInfo: NodeInfo) -> None:
        """create a node with the information given in a nodeInfo object"""
        self._nodeNum = nodeInfo._nodeNum
        self._nodeInfo = nodeInfo
        return None

    def createFromMyInfo(self, myInfo: dict) -> int:
        """create a node with the information given in a myInfo dictionary
        This case will always happen for the local node"""
        self._nodeNum = myInfo['my_node_num']
        self._data['my_info'] = myInfo
        self._isLocal = True
        return self._nodeNum

class MeshModel:
    """Keeps all data about the mesh entities during execution of a command."""

    def __init__(self) -> None:
        self.nodes: dict[int, Node] | None = None
        self.localNodeNum: int = -1
        self.localNode: Node | None = None

    def getLocalNode(self) -> Node | None:
        """returns the local node"""
        return self.nodes.get(self.localNodeNum)

    def getNode(self, nodeNum: int) -> Node | None:
        """returns the node with the given nodeNum"""
        return self.nodes.get(nodeNum)

    def addNode(self, node: Node, overwrite: bool = True) -> None:
        """adds the node information to the dictionary of nodes.
        if overwrite is True: silently overwrite any existing entry. Otherwise raise an error"""
        if nn := node.getNodeNum in self.nodes and not overwrite:
            raise RuntimeError(f'Node {node} cannot added because it already exists in the mesh')
        self.nodes[nn] = node
