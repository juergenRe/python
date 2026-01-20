"""Protocol handler for all packets related to logging"""
import io
import sys
from typing import Any
import logging

from google.protobuf.message import Message

try:
    import print_color  # type: ignore[import-untyped]
except ImportError as e:
    print_color = None

from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import topic_map
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protocol_base import ProtocolHandlerBase, LOGGING_HANDLER

from meshtastic.protobuf import mesh_pb2

logger = logging.getLogger(__name__)


@ProtocolHandlerBase.register
class LoggingHandler(ProtocolHandlerBase):
    """Protocol handler for handling log messages from radio"""
    def __init__(self, ifMesh: MeshInterface, debugOut: io.TextIOWrapper | None, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = LOGGING_HANDLER
        self.debugOut: io.TextIOWrapper | None = debugOut
        self.colorEncoding: dict = {
            'DEBUG': 'cyan',
            'INFO': 'white',
            'WARN': 'yellow',
            'ERR': 'red',
            'CRIT': 'red'
        }

    def receivePacket(self, field: str, packet: Message | str) -> None:
        record = '??? Unknown record'
        if isinstance(packet, mesh_pb2.FromRadio):         # got a log as mesh packet: decode it to string first
            if packet.HasField('log_record'):
                record = packet.log_record.message
        elif isinstance(packet, str):
            record = packet

        record = record.rstrip('\n')
        self.printLogLine(record)
        pub.sendMessage(topic_map.TOPIC_LOG_LINE, message=record, interface=self.ifMesh)

    def sendPacket(self, data: dict, **kwargs) -> Any:
        pass

    def closeHandler(self) -> Any:
        pass

    def printLogLine(self, line):
        """Print a line of log output. When output is stdout, color the log, otherwise just write to the file"""
        if self.debugOut is not None:
            if print_color is not None and self.debugOut == sys.stdout:
                color = 'black'
                for level, c in self.colorEncoding.items():
                    if level in line:
                        color = c
                        break
                print_color.print(line, color=color, end=None)
            else:
                self.debugOut.write(f'{line}\n')

