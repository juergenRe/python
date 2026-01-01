"""Object to manage the execution of a sequence of commands"""
import logging
import threading
import time
from collections import deque
from typing import Any

from meshtastic.protocol_manager import ProtocolHandlerManager
from meshtastic.commands import Command, GetConfigCommand
from meshtastic.mesh_model import MeshModel

from meshtastic import topic_map
from pubsub import pub  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class CommandExecutor:
    """Object to manage the execution of a sequence of commands"""
    def __init__(self, phm: ProtocolHandlerManager, meshModel: MeshModel, timeout: int = 300) -> None:
        self.phm: ProtocolHandlerManager = phm
        self.timeout: int = timeout
        self.meshModel: MeshModel = meshModel
        self.meshStatus: dict | None = None

    def execute(self, commands: list[Command]):
        """Executes the given list of commands"""

        pub.subscribe(self.onStatusReceive, topic_map.SUBS_MI_STATUS_PUB)
        pub.sendMessage(topic_map.SUBS_MI_STATUS_REQ)
        logger.debug(f"Starting execution of commands")
        try:
            while self.meshStatus is None:
                time.sleep(0.1)
            if not self.meshStatus['isConnected']:
                cmd = GetConfigCommand(None, None, [])
                data = cmd.execute(self.timeout)
                logger.debug(f"GetConfig data: {data}")
        except Exception as ex:
            logger.debug(f"GetConfig exception: {ex}")
        logger.debug(f"Finishing execution of commands")

    def onStatusReceive(self, data: dict):
        logger.debug(f"Sub: Received Status: {data}")
        self.meshStatus = data