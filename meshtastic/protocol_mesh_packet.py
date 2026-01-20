"""Protocol handler for all packets containing channel data"""
import enum
from typing import Any, NamedTuple, Callable
import logging

from google.protobuf.message import Message
from meshtastic.protobuf import mesh_pb2, portnums_pb2, admin_pb2
from google.protobuf.json_format import ParseDict, MessageToDict

from pubsub import pub  # type: ignore[import-untyped]

from meshtastic import topic_map, BROADCAST_NUM, stripnl
from meshtastic.mesh_interface import MeshInterface
from meshtastic.protocol_base import ProtocolHandlerBase, formatFieldName, MESH_PACKET_HANDLER
from meshtastic.mesh_model import ROLE_PRIMARY, ROLE_SECONDARY, ROLE_NONE
from meshtastic.topic_map import SUBS_PACKET_REQ, SUBS_PACKET_PUB

logger = logging.getLogger(__name__)


class KnownPacketType(NamedTuple):
    """Used to automatically decode known protocol payloads"""

    #: A descriptive name (e.g. "text", "user", "admin")
    name: str
    #: If set, will be called to parse as a protocol buffer
    protobufFactory: Callable | None
    #: If set, invoked as onReceive(interface, packet)
    onReceive: Callable
    onSend: Callable


@ProtocolHandlerBase.register
class MeshPacketHandler(ProtocolHandlerBase):
    """Protocol handler for all packets containing regular mesh data"""
    def __init__(self, ifMesh: MeshInterface, **kwargs) -> None:
        super().__init__(ifMesh)
        self.hdlrType: str | None = MESH_PACKET_HANDLER
        self.packetTypes = self.setPacketTypes()

        pub.subscribe(self.sendPacket, SUBS_PACKET_REQ)

    def setPacketTypes(self):
        """Well known message payloads can register decoders for automatic protobuf parsing"""
        knownPackets = {
        portnums_pb2.PortNum.UNKNOWN_APP:
            KnownPacketType("default", None, self._onStdReceive, self._onStdSend),
        portnums_pb2.PortNum.TEXT_MESSAGE_APP:
            KnownPacketType("text", None, self._onTextReceive, self._onTextSend),
        portnums_pb2.PortNum.ADMIN_APP:
            KnownPacketType("admin", admin_pb2.AdminMessage, self._onAdminReceive, self._onAdminSend),
        # portnums_pb2.PortNum.RANGE_TEST_APP:
        #     KnownPacketType("rangetest", onReceive=_onTextReceive),
        # portnums_pb2.PortNum.DETECTION_SENSOR_APP:
        #     KnownPacketType("detectionsensor", onReceive=_onTextReceive),
        # portnums_pb2.PortNum.POSITION_APP:
        #     KnownPacketType("position", mesh_pb2.Position, _onPositionReceive),
        # portnums_pb2.PortNum.NODEINFO_APP:
        #     KnownPacketType("user", mesh_pb2.User, _onNodeInfoReceive),
        # portnums_pb2.PortNum.ROUTING_APP:
        #     KnownPacketType("routing", mesh_pb2.Routing),
        # portnums_pb2.PortNum.TELEMETRY_APP:
        #     KnownPacketType("telemetry", telemetry_pb2.Telemetry, _onTelemetryReceive),
        # portnums_pb2.PortNum.REMOTE_HARDWARE_APP:
        #     KnownPacketType("remotehw", remote_hardware_pb2.HardwareMessage),
        # portnums_pb2.PortNum.SIMULATOR_APP:
        #     KnownPacketType("simulator", mesh_pb2.Compressed),
        # portnums_pb2.PortNum.TRACEROUTE_APP:
        #     KnownPacketType("traceroute", mesh_pb2.RouteDiscovery),
        # portnums_pb2.PortNum.POWERSTRESS_APP:
        #     KnownPacketType("powerstress", powermon_pb2.PowerStressMessage),
        # portnums_pb2.PortNum.WAYPOINT_APP:
        #     KnownPacketType("waypoint", mesh_pb2.Waypoint),
        # portnums_pb2.PortNum.PAXCOUNTER_APP:
        #     KnownPacketType("paxcounter", paxcount_pb2.Paxcount),
        # portnums_pb2.PortNum.STORE_FORWARD_APP:
        #     KnownPacketType("storeforward", storeforward_pb2.StoreAndForward),
        # portnums_pb2.PortNum.NEIGHBORINFO_APP:
        #     KnownPacketType("neighborinfo", mesh_pb2.NeighborInfo),
        # portnums_pb2.PortNum.MAP_REPORT_APP:
        #     KnownPacketType("mapreport", mqtt_pb2.MapReport),
        }
        return knownPackets

    # ----------------------------------------------------------------------------------
    # Methods for treatment of a packet to send
    def _sendMeshPacket(
            self,
            meshPacket: mesh_pb2.MeshPacket,
            destinationId: int = BROADCAST_NUM,
            wantAck: bool = False,
            hopLimit: int = 3,
            pkiEncrypted: bool = False,
            publicKey: bytes | None = None
        ) -> str:
        """Send a MeshPacket to the specified node (or if unspecified, broadcast)"""

        toRadio = mesh_pb2.ToRadio()

        nodeNum: int = 0
        if destinationId is None:
            logger.error("Warning: destinationId must not be None. Nothing sent")
            return 'Error'
        nodeNum = destinationId

        meshPacket.to = nodeNum
        meshPacket.want_ack = wantAck

        meshPacket.hop_limit = hopLimit

        if pkiEncrypted:
            meshPacket.pki_encrypted = True

        if publicKey is not None:
            meshPacket.public_key = publicKey

        # if the user hasn't set an ID for this packet (likely and recommended),
        # we should pick a new unique ID so the message can be tracked.
        if meshPacket.id == 0:
            logger.error("Error: no packet ID provided. Aborting send.")
            return 'Error'

        toRadio.packet.CopyFrom(meshPacket)
        logger.debug(f"Sending packet: {stripnl(meshPacket)}")
        self.ifMesh.sendToRadio(toRadio)
        return 'OK'

    def sendPacket(self, data: dict, args: dict) -> str:
        """
        Encode a mesh packet with payload 'data' and parameters in 'kwargs and sends
        it to another node
            data: the data to send as Python dict. Need to be filled into the specific pb defined by portNum
        Keyword Arguments:
            packetId: unique id of the packet
            replyId: the ID of the message that this packet is a response to
            destinationId: nodeNum,  where to send this message (default: {BROADCAST_MU;})
            channelIndex: channel number to use
            portNum: the application portnum (similar to IP port numbers) of the destination,
                    see portnums.proto for a list
            priority: how urgent this packet will be treated
            wantAck: True if you want the message sent in a reliable manner (with retries and
                    ack/nak provided for delivery)
            wantResponse: True if you want the service on the other
                    side to send an application layer response
            onResponseAckPermitted: should the onResponse callback be called
                    for regular ACKs (True) or just data responses & NAKs (False)
                    Note that if the onResponse callback is called 'onAckNak' this
                    will implicitly be true.
            hopLimit: hop limit to use
        Returns a string indicating success or failure
        """
        packetId: int = args.get("packetId")
        replyId: int = args.get("replyId")
        destinationId: int = args.get("destinationId", BROADCAST_NUM)
        channelIndex: int = args.get("channelIndex", 0)
        portNum: portnums_pb2.PortNum.ValueType = args.get("portNum", None)
        priority: mesh_pb2.MeshPacket.Priority.ValueType = args.get("priority", mesh_pb2.MeshPacket.Priority.RELIABLE)
        wantAck:bool = args.get("wantAck", False)
        wantResponse:bool = args.get("wantResponse", False)
        onResponseAckPermitted: bool = args.get("onResponseAckPermitted", False)
        hopLimit: int = args.get("hopLimit")
        pkiEncrypted: bool = args.get("pkiEncrypted", False)
        publicKey: bytes = args.get("publicKey")

        if packetId is None:
            logger.error("Error: no packet ID provided. Aborting send")
            return 'Error'
        if portNum is None:
            logger.error("Error: no portNum provided. Aborting send.")
            return 'Error'

        # encode the data according to the packetType defined by the port number.
        pcktType: KnownPacketType = self.packetTypes.get(portNum, None)
        if pcktType is None:
            logger.error("Unknown packet type. Aborting send")
            return 'Error'

        if pcktType.protobufFactory is not None:
            p = pcktType.protobufFactory()
            payload = self._onStdSend(data, p)
        else:
            cb = pcktType.onSend
            payload = b''
            if cb is not None:
                payload: bytes = cb(data)

        logger.debug(f"len(data): {len(payload)} max allowed: {mesh_pb2.Constants.DATA_PAYLOAD_LEN}")
        if len(payload) > mesh_pb2.Constants.DATA_PAYLOAD_LEN:
            logger.error("Data payload too big. Aborting send")
            return 'Error'

        meshPacket = mesh_pb2.MeshPacket()
        meshPacket.channel = channelIndex
        meshPacket.decoded.payload = payload
        meshPacket.decoded.portnum = portNum
        meshPacket.decoded.want_response = wantResponse
        meshPacket.id = packetId
        if replyId is not None:
            meshPacket.decoded.reply_id = replyId
        if priority is not None:
            meshPacket.priority = priority

        err = self._sendMeshPacket(
            meshPacket, destinationId, wantAck=wantAck, hopLimit=hopLimit,
            pkiEncrypted=pkiEncrypted, publicKey=publicKey)
        return err

    def closeHandler(self) -> Any:
        pass

    def _onStdSend(self, data: dict, p: Message = None) -> bytes:
        """Default serialization of data dict using protobuf factory"""
        ParseDict(data, p, True)
        return p.SerializeToString()

    def _onAdminSend(self, data: dict) -> bytes:
        p = admin_pb2.AdminMessage()
        ParseDict(data, p, ignore_unknown_fields=True)
        return p.SerializeToString()

    def _onTextSend(self, data: dict) -> bytes:
        return b''

    # ----------------------------------------------------------------------------------
    # Methods for treatment of a received packet
    def receivePacket(self, field: str, packet: Message) -> None:
        """Receive a mesh packet via callback"""
        logger.debug(f"Received packet {field}: {stripnl(packet)}")
        packetDict = MessageToDict(packet).get(field)
        packetDict['raw'] = packet.packet       # Here we need to provide the name of the field as literal once
        if 'from' not in packetDict:
            packetDict['from'] = 0
        if 'to' not in packetDict:
            packetDict['to'] = 0
        data = packetDict.get('decoded')
        if data is None:
            logger.debug("Received unknow packet type.")
            topic = "meshtastic.receive"  # Generic unknown packet type
            # Todo: Add sendMessage for API
            return

        # Handle data: extract port num and sending id and handle payload
        defaultPortnum = portnums_pb2.PortNum.Name(portnums_pb2.PortNum.UNKNOWN_APP)
        if 'portnum' not in data:
            data['portnum'] = defaultPortnum
            logger.warning(f"portnum was not in decoded. Setting to:{data['portnum']}")
        requestId = data.get("requestId")
        packetDict['requestId'] = requestId
        if requestId is not None:
            logger.debug(f"Got a response for requestId {requestId}")
            self.ifMesh.ackPacket(requestId)

        # Decode raw payload according to handlers defined for the portnums
        rawPayload = packetDict['raw'].decoded.payload
        data['payload'] = rawPayload
        portNumInt = packetDict['raw'].decoded.portnum  # we want portnum as an int
        handler = self.packetTypes.get(portNumInt)
        if handler is None:
            # no specific handler defined: forward as data
            topic = f"meshtastic.receive.data.{defaultPortnum}"
            # Todo: Add sendMessage for API
            return

        # Convert to protobuf if possible
        if handler.protobufFactory is not None:
            pb = handler.protobufFactory()
            pb.ParseFromString(rawPayload)
            p = MessageToDict(pb)
            data[handler.name] = p
            # Also provide the protobuf raw
            data[handler.name]["raw"] = pb

        # Call specialized onReceive if defined
        if handler.onReceive is not None:
            handler.onReceive(packetDict)
        pub.sendMessage(SUBS_PACKET_PUB, field=field, data=packetDict, requestId=requestId)

    def _onAdminReceive(self, asDict):
        """Special auto parsing for received messages"""
        logger.debug(f"in _onAdminReceive() asDict:{stripnl(asDict)}")
        # if "decoded" in asDict and "from" in asDict and "admin" in asDict["decoded"]:
        #     adminMessage = asDict["decoded"]["admin"]["raw"]
        #     iface._getOrCreateByNum(asDict["from"])["adminSessionPassKey"] = adminMessage.session_passkey

    def _onTextReceive(self, asDict):
        """Special text auto parsing for received messages"""
        # We don't throw if the utf8 is invalid in the text message.  Instead we just don't populate
        # the decoded.data.text and we log an error message.  This at least allows some delivery to
        # the app and the app can deal with the missing decoded representation.
        #
        # Usually btw this problem is caused by apps sending binary data but setting the payload type to
        # text.
        logger.debug(f"in _onTextReceive() asDict:{asDict}")
        try:
            asBytes = asDict["decoded"]["payload"]
            asDict["decoded"]["text"] = asBytes.decode("utf-8")
        except Exception as ex:
            logger.error(f"Malformatted utf8 in text message: {ex}")
        # _receiveInfoUpdate(iface, asDict)

    def _onStdReceive(self, asDict):
        pass