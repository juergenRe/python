"""Execution of info command"""
import base64
import logging
import json
from typing import Any

from pubsub import pub  # type: ignore[import-untyped]

from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM
from meshtastic.mesh_model import MeshModel, Node, ROLE_DISABLED, ROLE_NONE
from meshtastic.command_interface import CmdError
from meshtastic.commands import Command, FIELD_MYINFO
from meshtastic.util import pskToString

logger = logging.getLogger(__name__)


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
            publicURL = node.getUrl(includeAll=False)
            adminURL = node.getUrl(includeAll=True)
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

    def execute(self, model: MeshModel, timeout: int, **kwargs) -> tuple:
        """Show human-readable summary about this object"""
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        if self.destinationNode == BROADCAST_ADDR:
            localNode: Node = model.localNode
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

