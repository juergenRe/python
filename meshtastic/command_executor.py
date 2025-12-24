"""Object to manage the execution of a sequence of commands"""
import logging
import threading
from collections import deque
from typing import Any

from meshtastic.mesh_interface import MeshInterface
from meshtastic.commands import Command, GetConfigCommand
from meshtastic.mesh_model import MeshModel

logger = logging.getLogger(__name__)


class CommandExecutor:
    """Object to manage the execution of a sequence of commands"""
    def __init__(self, interface: MeshInterface, meshModel: MeshModel, timeout: int = 300) -> None:
        self.ifce: MeshInterface = interface
        self.timeout: int = timeout
        self.meshModel: MeshModel = meshModel

    def execute(self, commands: list[Command]):
        """Executes the given list of commands"""
        logger.debug(f"Starting execution of commands")
        try:
            if not self.ifce.isConnected.is_set():
                cmd = GetConfigCommand(None, None, [])
                data = cmd.execute(self.ifce, self.timeout)
                logger.debug(f"GetConfig data: {data}")
        except Exception as ex:
            logger.debug(f"GetConfig exception: {ex}")
        logger.debug(f"Finishing execution of commands")
