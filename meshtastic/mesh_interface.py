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

import google.protobuf.json_format

from meshtastic.interface_factory import InterfaceFactory
from meshtastic.radio_interface import RadioInterfaceBase, IRadioInterface

try:
    import print_color  # type: ignore[import-untyped]
except ImportError as e:
    print_color = None

from pubsub import pub  # type: ignore[import-untyped]
from meshtastic import (
    BROADCAST_ADDR,
    BROADCAST_NUM,
    LOCAL_ADDR,
    NODELESS_WANT_CONFIG_ID,
    ResponseHandler,
    protocols,
    publishingThread,
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
        noNodes: bool = False,
        debugOut: TextIOWrapper | None = None,
        noProto: bool = False,
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
        self.debugOut = debugOut
        self.noProto: bool = noProto

        self.interface: IRadioInterface | None = None
        self.internalFields = ('id', 'rebooted', 'queueStatus')
        self.registeredHandlers: dict[str, Callable] = {}

        # self.nodes: Optional[Dict[str, Dict]] = None  # FIXME
        # self.isConnected: threading.Event = threading.Event()
        # self.localNode: meshtastic.node.Node = meshtastic.node.Node(
        #     self, -1, timeout=timeout
        # )  # We fixup nodenum later
        # self.myInfo: Optional[
        #     mesh_pb2.MyNodeInfo
        # ] = None  # We don't have device info yet
        # self.metadata: Optional[
        #     mesh_pb2.DeviceMetadata
        # ] = None  # We don't have device metadata yet
        # self.responseHandlers: Dict[
        #     int, ResponseHandler
        # ] = {}  # A map from request ID to the handler
        # self.failure = (
        #     None  # If we've encountered a fatal exception it will be kept here
        # )
        # self._acknowledgment: Acknowledgment = Acknowledgment()
        # random.seed()  # FIXME, we should not clobber the random seedval here, instead tell user they must call it
        # self.currentPacketId: int = random.randint(0, 0xFFFFFFFF)
        # self.nodesByNum: Optional[Dict[int, Dict]] = None
        # self.configId: Optional[int] = NODELESS_WANT_CONFIG_ID if noNodes else None
        # self.gotResponse: bool = False  # used in gpio read
        # self.mask: Optional[int] = None  # used in gpio read and gpio watch
        # self.queueStatus: Optional[mesh_pb2.QueueStatus] = None
        # self._localChannels = None
        #
        self.heartbeatTimer: Optional[threading.Timer] = None
        self.cmdCallback = None
        self.pendingCmd: dict[int, tuple] = {}      # msgId: (cmd, callback, timeout T/F)
        self.queue: collections.OrderedDict = collections.OrderedDict()

        kwargs = {'rcvCallback': self._handleFromRadio, 'logCallback': self._handleLogLine}
        self.interface = InterfaceFactory().createInterface(**ifceType, **kwargs)

        # # We could have just not passed in debugOut to MeshInterface, and instead told consumers to subscribe to
        # # the meshtastic.log.line publish instead.  Alas though changing that now would be a breaking API change
        # # for any external consumers of the library.
        # if debugOut:
        #     pub.subscribe(MeshInterface._printLogLine, "meshtastic.log.line")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        if exc_type is not None and exc_value is not None:
            logger.error(
                f"An exception of type {exc_type} with value {exc_value} has occurred"
            )
        if trace is not None:
            logger.error(f"Traceback:\n{''.join(traceback.format_tb(trace))}")
        self.close()

    # @staticmethod
    # def _printLogLine(line, interface):
    #     """Print a line of log output."""
    #     if print_color is not None and interface.debugOut == sys.stdout:
    #         # this isn't quite correct (could cause false positives), but currently our formatting differs between different log representations
    #         if "DEBUG" in line:
    #             print_color.print(line, color="cyan", end=None)
    #         elif "INFO" in line:
    #             print_color.print(line, color="white", end=None)
    #         elif "WARN" in line:
    #             print_color.print(line, color="yellow", end=None)
    #         elif "ERR" in line:
    #             print_color.print(line, color="red", end=None)
    #         else:
    #             print_color.print(line, end=None)
    #     else:
    #         interface.debugOut.write(line + "\n")

    def _handleLogLine(self, line: str) -> None:
        """Handle a line of log output from the device."""

        # Devices should _not_ be including a newline at the end of each log-line str (especially when
        # encapsulated as a LogRecord).  But to cope with old device loads, we check for that and fix it here:
        if line.endswith("\n"):
            line = line[:-1]

        pub.sendMessage("meshtastic.log.line", line=line, interface=self)

    # def _handleLogRecord(self, record: mesh_pb2.LogRecord) -> None:
    #     """Handle a log record which was received encapsulated in a protobuf."""
    #     # For now we just try to format the line as if it had come in over the serial port
    #     self._handleLogLine(record.message)
    #
    def connectAndGetConfig(self, cmdTxt: str, cb, timeout=300):
        """abstract method to be overwritten by subclasses"""
        msgId = self._startConfig()
        self.pendingCmd[msgId] = (cmdTxt, cb, timeout)
        if not self.noProto:
            self._waitConnected(timeout=timeout)
            self.waitForConfig()
        logger.debug(f"config received completely")
        data = {
            'metadata': self.metadata,
            'myInfo': self.myInfo,
            'nodes': self.nodesByNum,
            'localNode': self.localNode
        }
        cb(cmdTxt, data)
        return

    def close(self):
        """Shutdown this interface"""
        if self.heartbeatTimer:
            self.heartbeatTimer.cancel()

        self._sendDisconnect()

        if self.interface:
            self.interface.close()

    def registerHandler(self, fieldName: str, callback: Callable) -> bool:
        """register a protocol handler treating protobuf message with 'field'
        Do not register in case the field is used inside this class"""
        if fieldName not in self.internalFields:
            self.registeredHandlers[fieldName] = callback
            return True
        return False

    def unregisterHandler(self, fieldName: str) -> bool:
        """delete protocol handler from dict"""
        if fieldName in self.registeredHandlers:
            del self.registeredHandlers[fieldName]
            return True
        return False

    def sendHeartbeat(self):
        """Sends a heartbeat to the radio. Can be used to verify the connection is healthy."""
        p = mesh_pb2.ToRadio()
        p.heartbeat.CopyFrom(mesh_pb2.Heartbeat())
        self._sendToRadio(p)

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

    def _connected(self):
        """Called by this class to tell clients we are now fully connected to a node"""
        # (because I'm lazy) _connected might be called when remote Node
        # objects complete their config reads, don't generate redundant isConnected
        # for the local interface
        if not self.isConnected.is_set():
            self.isConnected.set()
            self._startHeartbeat()
            publishingThread.queueWork(
                lambda: pub.sendMessage(
                    "meshtastic.connection.established", interface=self
                )
            )

    # def _startConfig(self) -> int:
        # """Start device packets flowing"""
        # self.myInfo = None
        # self.nodes = {}  # nodes keyed by ID
        # self.nodesByNum = {}  # nodes keyed by nodenum
        # self._localChannels = (
        #     []
        # )  # empty until we start getting channels pushed from the device (during config)
        #
        # startConfig = mesh_pb2.ToRadio()
        # if self.configId is None or not self.noNodes:
        #     self.configId = random.randint(0, 0xFFFFFFFF)
        #     if self.configId == NODELESS_WANT_CONFIG_ID:
        #         self.configId = self.configId + 1
        # startConfig.want_config_id = self.configId
        # self._sendToRadio(startConfig)
        # return self.configId

    def _sendDisconnect(self):
        """Tell device we are done using it"""
        m = mesh_pb2.ToRadio()
        m.disconnect = True
        self._sendToRadio(m)

    def _queueHasFreeSpace(self) -> bool:
        # We never got queueStatus, maybe the firmware is old
        if self.queueStatus is None:
            return True
        return self.queueStatus.free > 0

    def _queueClaim(self) -> None:
        if self.queueStatus is None:
            return
        self.queueStatus.free -= 1

    def _sendToRadio(self, toRadio: mesh_pb2.ToRadio) -> None:
        """Send a ToRadio protobuf to the device using opened interface"""
        # logger.debug(f"Sending toRadio: {stripnl(toRadio)}")

        if not toRadio.HasField("packet"):
            # not a meshpacket -- send immediately, give queue a chance,
            # this makes heartbeat trigger queue
            self.interface.sendToRadioImpl(toRadio)
        else:
            # meshpacket --> queue
            self.queue[toRadio.packet.id] = toRadio

        resentQueue = collections.OrderedDict()

        while self.queue:
            # logger.warn("queue: " + " ".join(f'{k:08x}' for k in self.queue))
            while not self._queueHasFreeSpace():
                logger.debug("Waiting for free space in TX Queue")
                time.sleep(0.5)
            try:
                packetId, packet = self.queue.popitem(last=False)       # ensures FIFO behavior
            except KeyError:
                break
            # logger.warn(f"packet: {packetId:08x} {packet}")
            resentQueue[packetId] = packet
            if packet is False:         # fixme: what does this test exactly?
                continue
            self._queueClaim()
            if packet != toRadio:
                logger.debug(f"Resending packet ID {packetId:08x} {packet}")
            self._sendToRadioImpl(packet)

        # logger.warn("resentQueue: " + " ".join(f'{k:08x}' for k in resentQueue))
        for packetId, packet in resentQueue.items():
            if self.queue.pop(packetId, False) is False:  # Packet got acked under us
                logger.debug(f"packet {packetId:08x} got acked under us")
                continue
            if packet:
                self.queue[packetId] = packet
        # logger.warn("queue + resentQueue: " + " ".join(f'{k:08x}' for k in self.queue))

    def _handleConfigComplete(self) -> None:
        """
        Done with initial config messages, now send regular MeshPackets
        to ask for settings and channels
        """
        # This is no longer necessary because the current protocol statemachine has already proactively sent us the locally visible channels
        # self.localNode.requestChannels()
        self.localNode.setChannels(self._localChannels)

        # the following should only be called after we have settings and channels
        self._connected()  # Tell everyone else we are ready to go

        # call back to requestor command
        cmdTxt, cb, timeout = self.pendingCmd[self.configId]
        del self.pendingCmd[self.configId]
        data = {
            'metadata': self.metadata,
            'myInfo': self.myInfo,
            'nodes': self.nodesByNum,
            'localNode': self.localNode
        }
        cb(cmdTxt, data)


    def _handleQueueStatusFromRadio(self, queueStatus) -> None:
        self.queueStatus = queueStatus
        logger.debug(
            f"TX QUEUE free {queueStatus.free} of {queueStatus.maxlen}, res = {queueStatus.res}, id = {queueStatus.mesh_packet_id:08x} "
        )

        if queueStatus.res:
            return

        # logger.warn("queue: " + " ".join(f'{k:08x}' for k in self.queue))
        justQueued = self.queue.pop(queueStatus.mesh_packet_id, None)

        if justQueued is None and queueStatus.mesh_packet_id != 0:
            self.queue[queueStatus.mesh_packet_id] = False
            logger.debug(
                f"Reply for unexpected packet ID {queueStatus.mesh_packet_id:08x}"
            )
        # logger.warn("queue: " + " ".join(f'{k:08x}' for k in self.queue))

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

        asDict = google.protobuf.json_format.MessageToDict(fromRadio)
        logger.debug(f"Received from radio: {fromRadio}")
        if fromRadio.HasField("my_info"):
            self.myInfo = fromRadio.my_info
            self.localNode.nodeNum = self.myInfo.my_node_num
            logger.debug(f"Received myinfo: {stripnl(fromRadio.my_info)}")

        elif fromRadio.HasField("metadata"):
            self.metadata = fromRadio.metadata
            logger.debug(f"Received device metadata: {stripnl(fromRadio.metadata)}")

        elif fromRadio.HasField("node_info"):
            logger.debug(f"Received nodeinfo: {asDict['nodeInfo']}")

            node = self._getOrCreateByNum(asDict["nodeInfo"]["num"])
            node.update(asDict["nodeInfo"])
            try:
                newpos = self._fixupPosition(node["position"])
                node["position"] = newpos
            except:
                logger.debug("Node without position")

            # no longer necessary since we're mutating directly in nodesByNum via _getOrCreateByNum
            # self.nodesByNum[node["num"]] = node
            if "user" in node:  # Some nodes might not have user/ids assigned yet
                if "id" in node["user"]:
                    self.nodes[node["user"]["id"]] = node
            publishingThread.queueWork(
                lambda: pub.sendMessage(
                    "meshtastic.node.updated", node=node, interface=self
                )
            )
        elif fromRadio.config_complete_id == self.configId:
            # we ignore the config_complete_id, it is unneeded for our
            # stream API fromRadio.config_complete_id
            logger.debug(f"Config complete ID {self.configId}")
            self._handleConfigComplete()
        elif fromRadio.HasField("channel"):
            self._handleChannel(fromRadio.channel)
        elif fromRadio.HasField("packet"):
            self._handlePacketFromRadio(fromRadio.packet)
        elif fromRadio.HasField("log_record"):
            self._handleLogRecord(fromRadio.log_record)
        elif fromRadio.HasField("queueStatus"):
            self._handleQueueStatusFromRadio(fromRadio.queueStatus)
        elif fromRadio.HasField("clientNotification"):
            publishingThread.queueWork(
                lambda: pub.sendMessage(
                    "meshtastic.clientNotification",
                    notification=fromRadio.clientNotification,
                    interface=self,
                )
            )

        elif fromRadio.HasField("mqttClientProxyMessage"):
            publishingThread.queueWork(
                lambda: pub.sendMessage(
                    "meshtastic.mqttclientproxymessage",
                    proxymessage=fromRadio.mqttClientProxyMessage,
                    interface=self,
                )
            )

        elif fromRadio.HasField("xmodemPacket"):
            publishingThread.queueWork(
                lambda: pub.sendMessage(
                    "meshtastic.xmodempacket",
                    packet=fromRadio.xmodemPacket,
                    interface=self,
                )
            )

        elif fromRadio.HasField("rebooted") and fromRadio.rebooted:
            # Tell clients the device went away.  Careful not to call the overridden
            # subclass version that closes the serial port
            MeshInterface._disconnected(self)

            self._startConfig()  # redownload the node db etc...

        elif fromRadio.HasField("config") or fromRadio.HasField("moduleConfig"):
            if fromRadio.config.HasField("device"):
                self.localNode.localConfig.device.CopyFrom(fromRadio.config.device)
            elif fromRadio.config.HasField("position"):
                self.localNode.localConfig.position.CopyFrom(fromRadio.config.position)
            elif fromRadio.config.HasField("power"):
                self.localNode.localConfig.power.CopyFrom(fromRadio.config.power)
            elif fromRadio.config.HasField("network"):
                self.localNode.localConfig.network.CopyFrom(fromRadio.config.network)
            elif fromRadio.config.HasField("display"):
                self.localNode.localConfig.display.CopyFrom(fromRadio.config.display)
            elif fromRadio.config.HasField("lora"):
                self.localNode.localConfig.lora.CopyFrom(fromRadio.config.lora)
            elif fromRadio.config.HasField("bluetooth"):
                self.localNode.localConfig.bluetooth.CopyFrom(
                    fromRadio.config.bluetooth
                )
            elif fromRadio.config.HasField("security"):
                self.localNode.localConfig.security.CopyFrom(
                    fromRadio.config.security
                )
            elif fromRadio.moduleConfig.HasField("mqtt"):
                self.localNode.moduleConfig.mqtt.CopyFrom(fromRadio.moduleConfig.mqtt)
            elif fromRadio.moduleConfig.HasField("serial"):
                self.localNode.moduleConfig.serial.CopyFrom(
                    fromRadio.moduleConfig.serial
                )
            elif fromRadio.moduleConfig.HasField("external_notification"):
                self.localNode.moduleConfig.external_notification.CopyFrom(
                    fromRadio.moduleConfig.external_notification
                )
            elif fromRadio.moduleConfig.HasField("store_forward"):
                self.localNode.moduleConfig.store_forward.CopyFrom(
                    fromRadio.moduleConfig.store_forward
                )
            elif fromRadio.moduleConfig.HasField("range_test"):
                self.localNode.moduleConfig.range_test.CopyFrom(
                    fromRadio.moduleConfig.range_test
                )
            elif fromRadio.moduleConfig.HasField("telemetry"):
                self.localNode.moduleConfig.telemetry.CopyFrom(
                    fromRadio.moduleConfig.telemetry
                )
            elif fromRadio.moduleConfig.HasField("canned_message"):
                self.localNode.moduleConfig.canned_message.CopyFrom(
                    fromRadio.moduleConfig.canned_message
                )
            elif fromRadio.moduleConfig.HasField("audio"):
                self.localNode.moduleConfig.audio.CopyFrom(fromRadio.moduleConfig.audio)
            elif fromRadio.moduleConfig.HasField("remote_hardware"):
                self.localNode.moduleConfig.remote_hardware.CopyFrom(
                    fromRadio.moduleConfig.remote_hardware
                )
            elif fromRadio.moduleConfig.HasField("neighbor_info"):
                self.localNode.moduleConfig.neighbor_info.CopyFrom(
                    fromRadio.moduleConfig.neighbor_info
                )
            elif fromRadio.moduleConfig.HasField("detection_sensor"):
                self.localNode.moduleConfig.detection_sensor.CopyFrom(
                    fromRadio.moduleConfig.detection_sensor
                )
            elif fromRadio.moduleConfig.HasField("ambient_lighting"):
                self.localNode.moduleConfig.ambient_lighting.CopyFrom(
                    fromRadio.moduleConfig.ambient_lighting
                )
            elif fromRadio.moduleConfig.HasField("paxcounter"):
                self.localNode.moduleConfig.paxcounter.CopyFrom(
                    fromRadio.moduleConfig.paxcounter
                )

        else:
            logger.debug("Unexpected FromRadio payload")

    def _fixupPosition(self, position: Dict) -> Dict:
        """Convert integer lat/lon into floats

        Arguments:
            position {Position dictionary} -- object to fix up
        Returns the position with the updated keys
        """
        if "latitudeI" in position:
            position["latitude"] = float(position["latitudeI"] * Decimal("1e-7"))
        if "longitudeI" in position:
            position["longitude"] = float(position["longitudeI"] * Decimal("1e-7"))
        return position

    def _nodeNumToId(self, num: int, isDest = True) -> Optional[str]:
        """Map a node node number to a node ID

        Arguments:
            num {int} -- Node number
            isDest {bool} -- True if the node number is a destination (to show broadcast address or unknown node)

        Returns:
            string -- Node ID
        """
        if num == BROADCAST_NUM:
            if isDest:
                return BROADCAST_ADDR
            else:
                return "Unknown"

        try:
            return self.nodesByNum[num]["user"]["id"]  # type: ignore[index]
        except:
            logger.debug(f"Node {num} not found for fromId")
            return None

    def _getOrCreateByNum(self, nodeNum):
        """Given a nodenum find the NodeInfo in the DB (or create if necessary)"""
        if nodeNum == BROADCAST_NUM:
            raise MeshInterface.MeshInterfaceError(
                "Can not create/find nodenum by the broadcast num"
            )

        if nodeNum in self.nodesByNum:
            return self.nodesByNum[nodeNum]
        else:
            presumptive_id = f"!{nodeNum:08x}"
            n = {
                "num": nodeNum,
                "user": {
                    "id": presumptive_id,
                    "longName": f"Meshtastic {presumptive_id[-4:]}",
                    "shortName": f"{presumptive_id[-4:]}",
                    "hwModel": "UNSET",
                },
            }  # Create a minimal node db entry
            self.nodesByNum[nodeNum] = n
            return n

    def _handleChannel(self, channel):
        """During initial config the local node will proactively send all N (8) channels it knows"""
        self._localChannels.append(channel)

    def _handlePacketFromRadio(self, meshPacket, hack=False):
        """Handle a MeshPacket that just arrived from the radio

        hack - well, since we used 'from', which is a python keyword,
               as an attribute to MeshPacket in protobufs,
               there really is no way to do something like this:
                    meshPacket = mesh_pb2.MeshPacket()
                    meshPacket.from = 123
               If hack is True, we can unit test this code.

        Will publish one of the following events:
        - meshtastic.receive.text(packet = MeshPacket dictionary)
        - meshtastic.receive.position(packet = MeshPacket dictionary)
        - meshtastic.receive.user(packet = MeshPacket dictionary)
        - meshtastic.receive.data(packet = MeshPacket dictionary)
        """
        asDict = google.protobuf.json_format.MessageToDict(meshPacket)

        # We normally decompose the payload into a dictionary so that the client
        # doesn't need to understand protobufs.  But advanced clients might
        # want the raw protobuf, so we provide it in "raw"
        asDict["raw"] = meshPacket

        # from might be missing if the nodenum was zero.
        if not hack and "from" not in asDict:
            asDict["from"] = 0
            logger.error(
                f"Device returned a packet we sent, ignoring: {stripnl(asDict)}"
            )
            print(
                f"Error: Device returned a packet we sent, ignoring: {stripnl(asDict)}"
            )
            return
        if "to" not in asDict:
            asDict["to"] = 0

        # /add fromId and toId fields based on the node ID
        try:
            asDict["fromId"] = self._nodeNumToId(asDict["from"], False)
        except Exception as ex:
            logger.warning(f"Not populating fromId {ex}")
        try:
            asDict["toId"] = self._nodeNumToId(asDict["to"])
        except Exception as ex:
            logger.warning(f"Not populating toId {ex}")

        # We could provide our objects as DotMaps - which work with . notation or as dictionaries
        # asObj = DotMap(asDict)
        topic = "meshtastic.receive"  # Generic unknown packet type

        decoded = None
        portnum = portnums_pb2.PortNum.Name(portnums_pb2.PortNum.UNKNOWN_APP)
        if "decoded" in asDict:
            decoded = asDict["decoded"]
            # The default MessageToDict converts byte arrays into base64 strings.
            # We don't want that - it messes up data payload.  So slam in the correct
            # byte array.
            decoded["payload"] = meshPacket.decoded.payload

            # UNKNOWN_APP is the default protobuf portnum value, and therefore if not
            # set it will not be populated at all to make API usage easier, set
            # it to prevent confusion
            if "portnum" not in decoded:
                decoded["portnum"] = portnum
                logger.warning(f"portnum was not in decoded. Setting to:{portnum}")
            else:
                portnum = decoded["portnum"]

            topic = f"meshtastic.receive.data.{portnum}"

            # decode position protobufs and update nodedb, provide decoded version
            # as "position" in the published msg move the following into a 'decoders'
            # API that clients could register?
            portNumInt = meshPacket.decoded.portnum  # we want portnum as an int
            handler = protocols.get(portNumInt)
            # The decoded protobuf as a dictionary (if we understand this message)
            p = None
            if handler is not None:
                topic = f"meshtastic.receive.{handler.name}"

                # Convert to protobuf if possible
                if handler.protobufFactory is not None:
                    pb = handler.protobufFactory()
                    pb.ParseFromString(meshPacket.decoded.payload)
                    p = google.protobuf.json_format.MessageToDict(pb)
                    asDict["decoded"][handler.name] = p
                    # Also provide the protobuf raw
                    asDict["decoded"][handler.name]["raw"] = pb

                # Call specialized onReceive if necessary
                if handler.onReceive is not None:
                    handler.onReceive(self, asDict)

            # Is this message in response to a request, if so, look for a handler
            requestId = decoded.get("requestId")
            if requestId is not None:
                logger.debug(f"Got a response for requestId {requestId}")
                # We ignore ACK packets unless the callback is named `onAckNak`
                # or the handler is set as ackPermitted, but send NAKs and
                # other, data-containing responses to the handlers
                routing = decoded.get("routing")
                isAck = routing is not None and (
                    "errorReason" not in routing or routing["errorReason"] == "NONE"
                )
                # we keep the responseHandler in dict until we actually call it
                handler = self.responseHandlers.get(requestId, None)
                if handler is not None:
                    if (
                        (not isAck)
                        or handler.callback.__name__ == "onAckNak"
                        or handler.ackPermitted
                    ):
                        handler = self.responseHandlers.pop(requestId, None)
                        logger.debug(
                            f"Calling response handler for requestId {requestId}"
                        )
                        handler.callback(asDict)

        logger.debug(f"Publishing {topic}: packet={stripnl(asDict)} ")
        publishingThread.queueWork(
            lambda: pub.sendMessage(topic, packet=asDict, interface=self)
        )
