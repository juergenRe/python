"""Implements protocol manager class which is responsible to create all handlers"""
import logging
from typing import Any, Callable
from pathlib import Path
from importlib import import_module

from meshtastic.protobuf import mesh_pb2
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protocol_base import IProtocolHandler
from meshtastic.protocol_base import (
    UNKNOWN_HANDLER,
    START_CONFIG_HANDLER,
    LOGGING_HANDLER,
    CHANNEL_HANDLER,
    CONFIG_HANDLER
)

#from meshtastic.protocol_start_config import StartConfigHandler

logger = logging.getLogger(__name__)

# some fields will use the same handler to process data. This dict lists them.
# if no entry for a specific field, then use the field name as handler type (1:1 relation)
FIELD2HANDLERTYPE = {
    'my_info': START_CONFIG_HANDLER,
    'metadata': START_CONFIG_HANDLER,
    'node_info': START_CONFIG_HANDLER,
    'config': CONFIG_HANDLER,
    'config_complete_id': START_CONFIG_HANDLER,
    'moduleConfig': CONFIG_HANDLER,
    'channel': CHANNEL_HANDLER,
    'log_record': LOGGING_HANDLER
}

KNOWN_HANDLERS = [
    'meshtastic.protocol_base.NotImplementedHandler',
    'meshtastic.protocol_start_config.StartConfigHandler',
    'meshtastic.protocol_logging.LoggingHandler',
    'meshtastic.protocol_channel_data.ChannelHandler',
    'meshtastic.protocol_config_data.ConfigHandler'
]


class ProtocolHandlerManager:
    """Instantiates all protocol handlers according to the implemented handlers and the implemented protocols
    Any undefined handlers will be set to 'NotImplementedHandler'"""
    def __init__(self, ifMesh: MeshInterface) -> None:
        self.handlers: list[IProtocolHandler] = []
        self.ifMesh: MeshInterface = ifMesh

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for pbFieldName in self._findFields():
            self.ifMesh.unregisterHandler(pbFieldName)
        for hdlr in self.handlers:
            hdlr.closeHandler()

    def instantiateHandlers(self, **kwargs):
        """Create all needed handler objects and keep them in a list
        Register all handlers according their type with the mesh interface"""
        self.handlers = self._createActualHandlers(**kwargs)
        handlerTypes = { hdlr.handlerType: hdlr for hdlr in self.handlers}

        refusedHandlers = []
        pbFields = self._findFields()
        for pbFieldName in pbFields:
            actHandlerType = FIELD2HANDLERTYPE.get(pbFieldName, pbFieldName)
            if actHandlerType in handlerTypes.keys():
                result = self.ifMesh.registerHandler(pbFieldName, handlerTypes[actHandlerType].receivePacket)
                if not result:   # MeshInterface refuses to register this handler
                    refusedHandlers.append(pbFieldName)
            else:
                result = self.ifMesh.registerHandler(pbFieldName, handlerTypes[UNKNOWN_HANDLER].receivePacket)
        self._removeRefusedHandlers(refusedHandlers)

    def _removeRefusedHandlers(self, refusedHandlers: list[str]) -> None:
        """remove all handlers from self.handlers which are refused by the client"""
        for i, handler in enumerate(self.handlers):
            if handler.handlerType in refusedHandlers:
                logger.debug(f"Removing refused handler {handler}")
                rh = self.handlers.pop(i)
                del rh

    def _findFields(self) -> list[str]:
        """Find all fields in pb2.FromRadio protobuf"""
        fields_raw = mesh_pb2.FromRadio().DESCRIPTOR.fields_by_name
        return [name for name in fields_raw.keys()]

    def _createActualHandlers(self, **kwargs) -> list[IProtocolHandler]:
        """find all implemented protocol handler classes and return their names"""
        # base: type[IProtocolHandler] = IProtocolHandler.__subclasses__()[0]
        # handlerClasses = base.__subclasses__()
        handlerClasses = KNOWN_HANDLERS

        handlers: list[IProtocolHandler] = []
        for name in handlerClasses:
            moduleName, _, clsName = name.rpartition('.')
        # for phClass in handlerClasses:
            # moduleName = phClass.__module__
            # clsName = phClass.__name__
            try:
                module = import_module(moduleName)
                cls = getattr(module, clsName)(self.ifMesh, **kwargs)
                handlers.append(cls)
            except Exception as ex:
                logger.debug(f"Error instantiating protocol handler {clsName}: {ex}")
                raise RuntimeError(f"Error instantiating protocol handler {clsName}: {ex}")
        return handlers
