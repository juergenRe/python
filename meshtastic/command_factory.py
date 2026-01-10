"""Creates a list of command objects out of the arguments passed to meshtastic CLI"""

from typing import Union
from types import ModuleType
import logging
from importlib import import_module

from meshtastic.util import snake_to_camel
from meshtastic.commands import *

logger = logging.getLogger(__name__)

noCommandArgs = [
    'support', 'port', 'host', 'ble', 'ble_scan', 'dest', 'ch_index',
    'channel_fetch_attempts', 'private', 'deprecated',
    'seriallog', 'ack', 'timeout', 'no_nodes', 'noproto',
    'debug', 'debuglib', 'test', 'wait_to_disconnect', 'listen', 'no_time',
    'power_riden', 'power_ppk2_meter', 'power_ppk2_supply', 'power_sim', 'power_voltage',
    'power_stress', 'power_wait', 'slog'
]
class CommandFactory:
    """Creates a list of command objects out of the arguments passed to meshtastic CLI"""
    def __init__(self, argsList: list[str]):
        """set up the list of arguments and their associated command class name"""
        self.cmdDict = {}
        for cmdArg in argsList:
            if cmdArg not in noCommandArgs:
                cmdPrefix = snake_to_camel(cmdArg.capitalize())
                self.cmdDict[cmdArg] = f"{cmdPrefix}Command"

    def createCommandList(self, argDict) -> list[Command]:
        """scans through the arguments and create command for each entry"""
        cmdList = []
        destNode = argDict['dest'] if argDict.get('dest') else BROADCAST_ADDR
        chIndex = int(argDict['ch_index']) if argDict.get('ch_index') else None
        for argName, params in argDict.items():
            # check for set arguments, which are commands
            if argName not in noCommandArgs and params is not None:
                if isinstance(params, list):
                    for param in params:
                        cmdList.append(
                            self.instantiateCommand(
                                self.cmdDict.get(argName, 'UnknownCommand'),
                                destNode, chIndex, param)
                        )
                elif isinstance(params, bool) and params is False:
                    pass    # do nothing in case of non-set bool params
                else:
                    cmdList.append(
                        self.instantiateCommand(
                            self.cmdDict.get(argName, 'UnknownCommand'),
                            destNode, chIndex, params)
                    )
        return cmdList

    def instantiateCommand(self, cmdName: str, destNode, chIndex, param) -> Command:
        try:
            logger.debug(f"creating Command for {cmdName} with params: {param}")
            # module_path, class_name = class_str.rsplit('.', 1)
            module = import_module('meshtastic.commands')
            return getattr(module, cmdName)(destNode, chIndex, param)
        except (ImportError, AttributeError) as e:
            raise ImportError(cmdName)
