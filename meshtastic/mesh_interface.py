"""Mesh Interface class
"""
# pylint: disable=R0917,C0302

import collections
import json
import logging
import math
import random
import secrets
import sys
import threading
import time
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union
from io import TextIOWrapper
from dataclasses import dataclass, asdict

import google.protobuf.json_format
from google.protobuf.json_format import MessageToDict

from meshtastic.interface_factory import InterfaceFactory
from meshtastic.protocol_interface import IProtocolHandler
from meshtastic.radio_interface import RadioInterfaceBase, IRadioInterface

try:
    import print_color  # type: ignore[import-untyped]
except ImportError as e:
    print_color = None

from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import topic_map

from meshtastic import (
    BROADCAST_ADDR,
    BROADCAST_NUM,
    LOCAL_ADDR,
    NODELESS_WANT_CONFIG_ID,
    ResponseHandler,
    protocols,
    publishingThread,
)
from meshtastic.util import (
    Acknowledgment,
    Timeout,
    convert_mac_addr,
    message_to_json,
    our_exit,
    remove_keys_from_dict,
    stripnl,
)

from meshtastic.protobuf import mesh_pb2, portnums_pb2, telemetry_pb2
from meshtastic.util import Acknowledgment, Timeout, convert_mac_addr, message_to_json, our_exit, remove_keys_from_dict, stripnl

logger = logging.getLogger(__name__)

def _timeago(delta_secs: int) -> str:
    """Convert a number of seconds in the past into a short, friendly string
    e.g. "now", "30 sec ago",  "1 hour ago"
    Zero or negative intervals simply return "now"
    """
    intervals = (
        ("year", 60 * 60 * 24 * 365),
        ("month", 60 * 60 * 24 * 30),
        ("day", 60 * 60 * 24),
        ("hour", 60 * 60),
        ("min", 60),
        ("sec", 1),
    )
    for name, interval_duration in intervals:
        if delta_secs < interval_duration:
            continue
        x = delta_secs // interval_duration
        plur = "s" if x > 1 else ""
        return f"{x} {name}{plur} ago"

    return "now"

@dataclass
class MeshInterfaceStatus:
    """keeps track of the current status data"""
    isConnected: bool = False
    tsConnected: datetime = datetime(1,1, 1)
    tsUnconnected: datetime = datetime(1,1, 1)
    rebooted: bool = True
    tsRebooted: datetime = datetime(1, 1, 1)

    def __repr__(self):
        sConn = 'Conn' if self.isConnected else '/Conn'
        sReboot = 'Reboot' if self.rebooted else '/Reboot'
        tsconn = '---' if self.tsConnected.year == 1 else self.tsConnected.isoformat(' ')
        tsunconn = '---' if self.tsUnconnected.year == 1 else self.tsUnconnected.isoformat(' ')
        tsboot = '---' if self.tsRebooted.year == 1 else self.tsRebooted.isoformat(' ')
        return f"{self.__class__.__name__}({sConn}@{tsconn}/{tsunconn}, {sReboot}@{tsboot}"

    def updateConnected(self, value: bool) -> None:
        """put new value of connection Status"""
        self.isConnected = value
        if value:
            self.tsConnected = datetime.now()
        else:
            self.tsUnconnected = datetime.now()

    def updateRebooted(self, value: bool) -> None:
        """put new value of rebooted Status"""
        self.rebooted = value
        if self.rebooted:       # only take timestamp when the reboot took place
            self.tsRebooted = datetime.now()


SEND_EVENT = 0
ACTSIZE_EVENT = 1

MAX_QUEUE = 16
START_THRESHOLD = MAX_QUEUE - 2
STOP_THRESHOLD = MAX_QUEUE // 2


@dataclass
class QueueStatus:
    """tracks QueueStatus message contents"""
    freeSpace: int = MAX_QUEUE
    maxSpace: int = MAX_QUEUE
    error: int = 0
    lastId: int = -1    # -1 indicates "no ID" known

    def sendPacket(self, xon: bool) -> bool:
        """Updates self.xon when sending data packets"""
        self.freeSpace -= 1

        if self.freeSpace < STOP_THRESHOLD:
            return False
        elif self.freeSpace >= START_THRESHOLD:
            return True
        return xon          # don't change value

    def update(self, d: dict) -> None:
        if 'free' in d:
            self.freeSpace = d['free']
        if 'maxlen' in d:
            self.maxSpace = d['maxlen']
        if 'error' in d:
            self.error = d['error']
        if 'lastId' in d:
            self.lastId = d['lastId']

class MeshInterface:  # pylint: disable=R0902
    """Interface class for meshtastic devices

    Properties:

    isConnected
    nodes
    debugOut
    """

    # class MeshInterfaceError(Exception):
    #     """An exception class for general mesh interface errors"""
    #
    #     def __init__(self, message):
    #         self.message = message
    #         super().__init__(self.message)
    #
    def __init__(
        self,
        ifceType: dict,
        timeout: int = 300,
        noNodes: bool = False
    ) -> None:
        """Constructor

        Keyword Arguments:
            noProto -- If True, don't try to run our protocol on the
                       link - just be a dumb serial client.
            noNodes -- If True, instruct the node to not send its nodedb
                       on startup, just other configuration information.
            timeout -- How long to wait for replies (default: 300 seconds)
        """
        self._timeout: Timeout = Timeout(maxSecs=timeout)
        self.noNodes: bool = noNodes

        self.interface: IRadioInterface | None = None
        self.configId: int | None = None
        self.registeredHandlers: dict[str, Callable] = {        # initialize handlers for use within this class
            'id': self._handleId,
            'rebooted': self._handleReboot,
            'queueStatus': self._handleQueueStatus
        }
        self.internalHandlers: list[str] = list(self.registeredHandlers.keys())
        self.status: MeshInterfaceStatus = MeshInterfaceStatus()
        self.heartbeatTimer: threading.Timer | None = None
        self.processingThread: threading.Thread
        self.stopProcessing: bool = False

        # Packet transmission validation
        self.pendingCmd: dict[int, tuple] = {}      # msgId: (cmd, time(end-Time))

        # Packet queuing
        self.txQueue: collections.deque = collections.deque(maxlen=100)
        self.ackId: collections.deque = collections.deque(maxlen=100)
        self.waitAckQueue: collections.OrderedDict = collections.OrderedDict()
        self.xon: bool = True       # is sending from txQueue permitted?
        self.qs: QueueStatus = QueueStatus()

        self.processingThread = self._createThread()
        kwargs = {'rcvCallback': self._handleFromRadio, 'logCallback': self._handleLogLine}
        self.interface = InterfaceFactory().createInterface(**ifceType, **kwargs)

        pub.subscribe(self.onStatusRequest, topic_map.SUBS_MI_STATUS_REQ)
        pub.subscribe(self.onConnected, topic_map.SUBS_MI_CONNECTED)
        pub.subscribe(self.onDisconnect, topic_map.SUBS_MI_DISCONNECT)
        logger.debug(f'Subscribing to topics: {(topic_map.SUBS_MI_STATUS_REQ, topic_map.SUBS_MI_CONNECTED, topic_map.SUBS_MI_DISCONNECT)}')

    def _createThread(self) -> threading.Thread:
        """Create and start the processing thread"""
        logger.debug("Thread starting")
        thd = threading.Thread(
            target=self._processing, name=f"{self.__class__.__name__}", daemon=True
        )
        thd.start()
        logger.debug("Thread running")
        return thd

    def _processing(self):
        while not self.stopProcessing:
            # send next item(s) from queue
            while self.xon and len(self.txQueue) > 0:
                toRadio = self.txQueue.popleft()
                if toRadio.HasField('packet'):
                    # we can only trace reception for mesh packets, others might be lost
                    self.waitAckQueue[toRadio.packet.id] = toRadio
                self.interface.sendToRadioImpl(toRadio)
                self.xon = self.qs.sendPacket(self.xon)

            # handle acknowledgements
            if len(self.ackId) > 0:
                ackedId = self.ackId.popleft()
                if ackedId in self.waitAckQueue:
                    del self.waitAckQueue[ackedId]
                    logger.debug(f"Packet {ackedId} is acknowledged")
                else:
                    logger.debug(f"Packet {ackedId} is not found in waitAckQueue")

            # handle time-outs of commands
            for k, v in self.pendingCmd.items():
                cmd, endTime = v
                if 0 < endTime < time.time():
                    logger.debug(f"Command {cmd} timed out")
                    endTime = 0

            time.sleep(0.1)
        logger.debug("Stop processing")

    @property
    def isConnected(self) -> bool:
        return self.status.isConnected

    def onStatusRequest(self) -> None:
        """returns actual interface status"""
        statusDict = asdict(self.status)
        # publishingThread.queueWork(
        #     lambda: pub.sendMessage(topic_map.SUBS_MI_STATUS_PUB, statusDict)
        # )
        pub.sendMessage(topic_map.SUBS_MI_STATUS_PUB, data=statusDict)
        logger.debug(f"got sub status request: return status {self.status} ")

    def onConnected(self) -> None:
        """Callback when the connection has established:
        - Start heartbeat
        - update status
        """
        self.status.updateConnected(True)
        pub.sendMessage(topic_map.SUBS_MI_STATUS_PUB, data=asdict(self.status))
        pub.sendMessage(topic_map.TOPIC_CONNECTED)
        self._startHeartbeat()
        logger.debug(f"Connected and heartbeat started {self.status}")

    def onDisconnect(self):
        """Callback for a disconnection request"""
        self.close()
        self.status.updateConnected(False)
        pub.sendMessage(topic_map.SUBS_MI_STATUS_PUB, data=asdict(self.status))
        pub.sendMessage(topic_map.TOPIC_DISCONNECTED)
        logger.debug(f"Disconnected, interface closed and heartbeat stopped {self.status}")

    def startConnection(self, cmdTxt: str, timeout: int = 300) -> int:
        """Establish connection to radio and get initial configuration"""
        self.interface.connect()
        if not self.processingThread.is_alive():
            self.processingThread = self._createThread()

        msg, msgId = self._createStartConfigMsg(self.configId, self.noNodes)
        self.pendingCmd[msgId] = (cmdTxt, time.time() + timeout)
        self.sendToRadio(msg)
        self.configId = msgId
        logger.debug(f"created start config msg using {msgId} ")
        return msgId

    def close(self):
        """Shutdown this interface"""
        if self.heartbeatTimer:
            self.heartbeatTimer.cancel()
        self._sendDisconnect()
        if self.interface:
            self.interface.close()

    def registerHandler(self, fieldName: str, hdlr: Callable) -> bool:
        """register a protocol handler object treating protobuf message with 'field'
        Do not register in case the field is used inside this class"""
        if fieldName not in self.registeredHandlers:
            self.registeredHandlers[fieldName] = hdlr
            return True
        return False

    def unregisterHandler(self, fieldName: str) -> bool:
        """delete protocol handler from dict"""
        if fieldName in self.registeredHandlers and fieldName not in self.internalHandlers:
            del self.registeredHandlers[fieldName]
            return True
        return False

    def ackPacket(self, packeId: int) -> None:
        """Acknowledge a received packet from protocol handler toward interface
        Take care to not interfere with threading"""
        self.ackId.append(packeId)

    def _createStartConfigMsg(self, actId: int, noNodes: bool) -> tuple:
        """create start config message
        FixMe: to be relocated to protocol handler, it has nothing to do with transport tasks
        """
        startConfig = mesh_pb2.ToRadio()
        if actId is None or not noNodes:
            actId = random.randint(0, 0xFFFFFFFF)
            if actId == NODELESS_WANT_CONFIG_ID:
                actId = actId + 1
        startConfig.want_config_id = actId
        return startConfig, actId

    def sendHeartbeat(self):
        """Sends a heartbeat to the radio. Can be used to verify the connection is healthy."""
        p = mesh_pb2.ToRadio()
        p.heartbeat.CopyFrom(mesh_pb2.Heartbeat())
        self.sendToRadio(p)

    def _startHeartbeat(self):
        """We need to send a heartbeat message to the device every X seconds"""

        def callback():
            self.heartbeatTimer = None
            interval = 300
            logger.debug(f"Sending heartbeat, interval {interval} seconds")
            self.heartbeatTimer = threading.Timer(interval, callback)
            self.heartbeatTimer.start()
            self.sendHeartbeat()

        callback()  # run our periodic callback now, it will make another timer if necessary

    def _sendDisconnect(self):
        """Tell device we are done using it"""
        m = mesh_pb2.ToRadio()
        m.disconnect = True
        self.sendToRadio(m)

    def sendToRadio(self, toRadio: mesh_pb2.ToRadio) -> None:
        """Send a ToRadio protobuf to the device using opened interface"""
        # logger.debug(f"Sending toRadio: {stripnl(toRadio)}")

        if not toRadio.HasField("packet"):
            # not a meshpacket -- put in front to send immediately
            self.txQueue.appendleft(toRadio)
        else:
            # meshpacket --> queue
            self.txQueue.append(toRadio)

    def _handleLogLine(self, logline: str) -> None:
        """Capture serial log from radio and write it to the debug file"""
        cb = self.registeredHandlers.get('log_record')
        cb('log_record', logline)

    def _handleId(self):
        """dummy callback, do nothing here"""
        pass

    def _handleReboot(self):
        """handles the reboot information message"""
        self.status.updateRebooted(True)
        pub.sendMessage(topic_map.SUBS_MI_STATUS_PUB, data=asdict(self.status))
        logger.debug(f"Reboot message: {self.status}")

    def _handleQueueStatus(self, field: str, packet: mesh_pb2.FromRadio) -> None:
        """Handle the QueueStatus message"""
        d = MessageToDict(packet.queueStatus)
        self.qs.update(d)
        logger.debug(f"{self.qs}")

        if 'res' in d:
            return

        # # logger.warn("queue: " + " ".join(f'{k:08x}' for k in self.queue))
        # justQueued = self.txQueue.pop(queueStatus.mesh_packet_id, None)
        #
        # if justQueued is None and queueStatus.mesh_packet_id != 0:
        #     self.txQueue[queueStatus.mesh_packet_id] = False
        #     logger.debug(
        #         f"Reply for unexpected packet ID {queueStatus.mesh_packet_id:08x}"
        #     )
        # # logger.warn("queue: " + " ".join(f'{k:08x}' for k in self.queue))

    def _handleFromRadio(self, fromRadioBytes):
        """Handle a packet that arrived from the radio. At this level
        only Heartbeat, QueueStatus and rebooted messages are treated.
        All others will be forwarded to specific protocol handlers
        This routine is a callback called from the interface implementation"""
        fromRadio = mesh_pb2.FromRadio()
        logger.debug(f"in mesh_interface.py _handleFromRadio() fromRadioBytes: {fromRadioBytes}")
        try:
            fromRadio.ParseFromString(fromRadioBytes)
        except Exception as ex:
            logger.error(f"Error while parsing FromRadio bytes:{fromRadioBytes} {ex}")
            traceback.print_exc()
            raise ex

        fields_raw = mesh_pb2.FromRadio().DESCRIPTOR.fields_by_name
        logger.debug(f"Dispatch Message {stripnl(fromRadio)}")
        for field in fields_raw.keys():
            try:
                if fromRadio.HasField(field):
                    cb = self.registeredHandlers[field]
                    cb(field, fromRadio)
            except ValueError:      # ignore HasField('id) exception
                if field != 'id':
                    logger.debug(f"Error while parsing FromRadio field:{field}")
            except Exception as ex:
                logger.debug(f"Error while checking for field {field} is contained in message {stripnl(fromRadio)} ex: {ex}")
