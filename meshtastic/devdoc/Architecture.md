# Meshtastic CLI Architecture

## Communication to Radio

### Level 1: Link connection 

This level should define the way the data is transmitted between
sender and receiver.

Basic format of data sent (e.g. over serial line):

The transmitted data is separated in a 4 Byte header and the data part.

| ByteNo | Value | Comment |
|--------|--|--|
| 1      | 0x94 | START1
| 2      | 0xC3 | START2
| 3      | LenH | High byte of data length 
| 4      | LenL | Low byte of data length
| 5      | Data 0 | first data byte
| ...    |   |
| N      | Data N | Last data byte

This format is used for both serial and TCP connections to the radio.
When using BLE, only the data itself will be transmitted. BLE is taking
care about the header (which basically also is true for TCP).

The data must be formatted as a "ToRadio" protobuf structure.

### Level 2: ToRadio/FromRadio packets

This level should define some control messages and otherwise feed through
different types of packets according to their "envelope".

Control Messages are:
- disconnect
- Heartbeat
- want_config_id (somehow in between, because it delivers a lot of data but it serves otherwise to start communication)
- config_complete_id (denotes the full availability of local radio)
- rebooted
- QueueStatus
- FileInfo



__ToRadio:__
Support 6 different packets:
1. MeshPacket (ID 1): a regular packet sent to the mesh
2. want_config_id (ID 3): requests actual config of local node, using a random ID
3. disconnect (ID4): announces a disconnection
4. XModem packet (ID 5)
5. MqttClientProxyMessage (ID 6)
6. Heartbeat (ID 7): to keep the connection active

__FromRadio:__
1. packet_id:   not clear if this is always filled in and what it exactly means
2. Payload with variants:
   3. MeshPacket
   4. MyNodeInfo (partial response to of want_config_id)
   5. NodeInfo (partial response to of want_config_id)
   6. Config (partial response to of want_config_id)
   7. LogRecord
   8. config_complete_id: returns initial sent ID when all config has been sent
   9. rebooted
   10. ModuleConfig (partial response to of want_config_id)
   11. Channel (partial response to of want_config_id)
   12. QueueStatus
   13. XModem
   14. DeviceMetaData (partial response to of want_config_id)
   15. MqttClientProxyMessage
   16. FileInfo
   17. ClientNotification
   18. DeviceUIConfig

This level should also handle the send/receive queue to ensure that no overflow 
happens on either side.

This means in particular, that the received QueueStatus messages from the radio are
interpreted and used to throttle/limit the flow of messages towards the radio.

On the lowest level exist the direct interfaces to the radios using the different 
data transmission protocols. Those are defined by the _IRadioInterface_

```plantuml
title Level 1 classes: Radio Interface definition

interface IRadioInterface {
    connect(addr)
    close()
    sendToRadioImpl()
    -receiveFromRadioImpl()
}

interface IRadioPacket <<protobuf>>{
    toRadio: pb
    fromRadio: pb
}

class RadioInterfaceBase <<abstract>>{
    rcvCallback
    logCallback
}

abstract class StreamInterface {
    -rxThread
    connect()
    close()
    {abstract}-readBytes()
    {abstract}-writeBytes()
    -receiveFromRadioImpl()
    -sendToRadioImpl()
}

class TCPInterface {
    hostname:portnumber
    socket
    open()
    close()
    -writeBytes()
    -readBytes()
}

class SerialInterface{
    address: str
    stream: Serial
    open()
    close()
    -writeBytes()
    -readBytes()
}

class BLEInterface {
    -receiveThread
    client: BLEClient
    -startConfig()
    -receiveFromRadioImpl()
    -sendToRadioImpl()
    connect()
    close()
}

class BLEClient <<async>> {
    connect()
    disconnect()
    close()
    discover()
}

class SimulatedInterface {
    -sendToRadio()
    -handleFromRadio()
    connect()
    disconnect()
}

note bottom of SimulatedInterface
    implements the case where one don't want to have
    real communication but want to test the remaining
    parts
end note

note right of SerialInterface::devPath
    serial port number
end note

RadioInterfaceBase .up.|> IRadioInterface: implements
BLEInterface -up-|> RadioInterfaceBase
StreamInterface -up-|> RadioInterfaceBase
SimulatedInterface -up-|> RadioInterfaceBase
SerialInterface -up-|> StreamInterface
TCPInterface -up-|> StreamInterface
IRadioInterface -left-> IRadioPacket: uses
BLEInterface "1" o-down- "1" BLEClient

```

```plantuml
title Level 2 classes: Interface to radio: Queuing, Heartbeat
interface IMeshInterface {
    __ functions __
    -handleFromRadio()
    -sendToRadio()
    connect()
    close()
    registerHandler()
    unregisterHandler()
    sendPacket()
    == creation parameters ==
    radioInterfaceType
    clientAddress: str
    timeout: int = 300
    noNodes: bool
}

interface IRadioPacket <<protobuf>>{
    toRadio: pb
    fromRadio: pb
}

class MeshInterface {
    queue
    heartbeatTimer
    status
    -sendToRadio()
    -handleFromRadio()
    startCommunication()
    disconnect()
    registerHandler()
    unregisterHandler()
    sendPacket()
}

class InterfaceFactory{
    selectInterface()
    createInterface()
}

note right of MeshInterface
    Interface is responsible to transfer the data, 
    but not process them.
    Responsibilities:
    - sending/receiving radio packets (toRadio/fromRadio)
    - manage rcv/snd queues
    - manage ack of msg id
    - send heartbeat
    - manage timeouts
end note

MeshInterface .up.|> IMeshInterface: implements
IMeshInterface -left-> IRadioPacket: uses
MeshInterface -right-> InterfaceFactory: "create Instance"
InterfaceFactory -down-> RadioInterface: instantiates
MeshInterface --> RadioInterface: uses


```
#### Status handling
Status data contains:
* isConnected: all initial data has been transferred from radio to client
* rebooted: timestamp of last reboot.
* error: Any error code including timestamp

Whenever a change in the status occurs, the new data will be published over pub-sub

#### Queue handling

This concerns mainly the transmit queue to prevent overflow of the radio 
processing queue. The radio transmits the current queue level with a
specific QueueStatus packet which then can be used in a XON/XOFF manner to
control the flow of data from MeshInterface to the radio.
Max queue places: N=16
Stop sending at: xoff = N-2 = 14
Resume sending at: xon = N/2 = 8

#### Control flow
Mesh packets contain a message ID, which can be used to validate that
the message has been processed correctly within the radio (Actually 
it happens quite often, that messages get lost). A lost package should be 
resent if no ACK is receieved within a resend time-out.

```plantuml
title Level 2: Control flow of radio packets
participant ProtocolHandler as ph
participant MeshInerface as mi
participant Queue as q
participant "Waiting \nAcknowledgements" as ack
participant RadioInterface as ri

== receive packet from higher level ==
ph -> mi: SendPacket
mi -> q: put
note right: when input q reaches a certain level, an error should be raised

== handle packets to send from queue ==
alt xon active
    mi <- q: pull
    mi -> ri: send
    mi -> ack: put (ID, timestamp, msg)
end

== handle acknowledgements ==
ri -> mi: getAck
mi -> mi: check ID
mi -> ack: remove

== handle time-outs ==
mi -> ack: check time-outs
alt timeout found
    mi <- ack: pull entry
    mi -> q: put msg
end

== handle queue levels ==
mi <- ri: receive QueueStatus
mi -> mi: set xon/xoff
```

### Level 3: Higher Level Packets

Those packets are:
- MeshPacket
- XModem
- MqttClientProxyMessage
- LogRecord
- ClientNotification
- Config-Data*
- ModuelConfig-Data*
- Channel-Data*
- initial config data: MyNodeInfo, NodeInfo, DeviceMetaData, DeviceUIConfig

*) Also received during initial config transmission

Those packets need to be decoded according to their content by the appropriate handler
Handlers should register themselves at the receiver, so they can called when
the respective packet arrives.

_Implementation note:_
Receiving data is currently executed in a separate receiving thread.
It seems to be useful to keep decoding data in the protocol handlers
within this thread, but switch back to the main thread when publishing
the decoded data.

The idea here is, that the handlers queue the decoded data in
the manager and set a flag. This will waken the main thread to take 
the data and publish it to the subscribed applications.

Might be useful to use async functions here.

```plantuml
title Level 2 classes: Handling of protocols
interface IMeshInterface {}

interface IProtocolHandler {
    MeshInterface
    ProtocolType
    receivePacket()
    sendPacket()
    addHandler()
    removeHandler()
}
class BaseProtocolHandler 
class MeshPacketHandler

class ProtocolManager{
    list: ProtocolHandler
    rcvQueue
    createHandler()
    sendData()
    receiveData()
    shutdown()
}


BaseProtocolHandler .up.|>IProtocolHandler: implements
BaseProtocolHandler <|-- MeshPacketHandler
BaseProtocolHandler <|-- MqttPacketHandler
BaseProtocolHandler <|-- StartConfigHandler 
BaseProtocolHandler <|-- IfcStatusHandler 
BaseProtocolHandler <|-- LoggingHandler 
BaseProtocolHandler <|-- UnknownHandler 
BaseProtocolHandler "1" o-> "1" IMeshInterface: "       "

ProtocolManager --> IProtocolHandler: create

note bottom of UnknownHandler
    used for every otherwise unknown 
    or not implemented protocol
end note

```


### Level 4: Applications

Those applications are:
* start communication and receive initial data
* CLI commands
* Logging data
* MQTT messages
* XModem
* other (Client Notification?)

In order to realize the communication between applications (which might be there or not)
and the protocol handlers, a Pub-Sub Model seems to be appropriate.

Using this model, any application can subscribe whenever it is active and then receives
the data it will use. Thus, the application will not need to know
much about the underlying capabilities: if something is missing, an error will be returned.

#### Pub-Sub Topics

Following topics will be defined:

Root topic: "meshtastic"
Child Topics: according to the applications discovered:
* "Logging"
* "MQTT"
* "XModem"
* "ClientNotification"
* "StartCommunication"
* "Command"

SubTopics of these are application specific:

| Child topic        | Sub topic      | Comment                                                       |
|--------------------|----------------|---------------------------------------------------------------|
| Logging            | Receive        | Logging app will receive a new entry                          |
| StartCommunication | Start          | Trigger communication start                                   |
|                    | Receive        | a new data part is received, the data will explain itself     |
|                    | Finish         | all data received, app can be terminated                      |
| Command            | Send           | send a request from application to protocol handler           |
|                    | Receive        | receive a response, this might be only part ot the whole data |
|                    | Finish         | all packets received, command can be terminated               |
| MeshInterface      | Status         | update message with new status data                           |
| MeshModel          | Config         | new configuration settings data                               |
|                    | ModuleConfig   | new module configuration settings data                        |
|                    | Channel        | new channel configuration data                                |
|                    | MyInfo         | new MyInfo data                                               |
|                    | NodeInfo       | new node info data                                            |
|                    | DeviceMetaData | new device meta data                                          |
|                    | DeviceUIConfig | new UI config data                                            |


Callables shall be named with the name of the Sub-topic, adding a prefix "on", e.g. "onReceive"

```plantuml
title Level 3 classes: Application "Command"

class CmdExecutor {
    iface
    MeshModel
}

class Cmd {
    environment
    destinationNode
    transactionNo
    cmd
    parameter
}

class CmdFactory <<singleton>> {
    createCmdList()
    createCmd()
}

class Starter {
    argumentParser
}

class MeshModel {
    localNode
    nodes
}

class Tunnel
class Node {
    nodeNum
    localConfig: pb
    moduleConfig: pb
    channels: list
}

class pub

class ProtocolManager <<singleton>>

ProtocolManager "1" -right-o "1" Starter
Cmd "N" *-- "1" MeshModel: uses
CmdExecutor "1" o-right- "N" Cmd: "executes   "
CmdFactory "1" -left-> "N" Cmd: "        "
Starter -up-> CmdFactory
MeshModel "1" *-- "N" Node: contains
Starter -left-> MeshModel: "creates "
Starter -up-> CmdExecutor: instantiate
ProtocolManager .up.> pub: "subs Command.Send >"
ProtocolManager ..> pub: "subs StartComm.Start >"
Cmd .up.> pub: "subs *.Receive >"
Cmd ..> pub: "subs *.Finish >"
note top of Cmd
    transactionNo will group commands together
    during sending of data. Value 0: no grouping
end note

note bottom of Starter
    represents the main program,
    collecting all arguments and
    passing them to the CmdFactory
    for the creation of a list of Cmd
end note

note bottom of ProtocolManager
    responsible to make the link between the protocol decoding
    and the application which needs this data
    Will be started once and run till shutdown, independent of 
    applications, which might appear and disappear in the meantime
end note
```
```plantuml
title Sequence for initiating reading initial configuration via a cmd
participant Cmd as cmd
participant ProtocolHandler as ph
collections MeshModel as mm
participant MeshInterface as mi
participant RadioInterface as ri

cmd --> cmd: subscribe to ProtocolHandler(s)
cmd -> ph: send "Start"
note right: using pub-sub 
ph -> ph: prepare protobuf
ph -> mi: send packet
mi -> ri: trigger rading threads
note right: to be done when reading the first time
mi -> ri: transfer ToRadio
note right: handle queue etc.
ri -[#red]> mi: transfer FromRadio
note right: handle internal messages directly
mi -[#red]> ph: callback with protobuf packet
ph -[#red]> ph: decode packet
ph -[#red]> cmd: callback with data
cmd --> cmd: Transfer data to main thread
cmd -> mm: collect data
== Other packets arriving ==
ri -> mi: transfer FromRadio
note right: receiving "config_complete"
mi -> ph: callback with protobuf packet
ph -> mm: get data
mm --> ph: collected data
ph -> cmd: send data
```

```plantuml
title Sequence after reboot of radio
note across 
config data needs to be read and forwarded to the app once the
radio is rebooting.
end note
participant CmdSet as cmd
participant ProtocolHandler as ph
collections MeshModel as mm
participant MeshInterface as mi
participant RadioInterface as ri

mi -> ri: Send packet
note right: this packet will force a reboot of the radio
ri -> mi: FromRadio
note right: signalling reboot
mi -> ph: callback
note right: need to signal a reboot
ph -> cmd: finished
note right: signal execution of command, but with a reboot.

```
#### Handling of Threads

Actually, the received data from a radio is returned in a different thread.
We thus need to ensure correct writing of data into the MeshModel,
because this one is designed to work in the main thread.

The idea here is to make the transfer of data from the receiving
thread into the main thread within the executing command:
* When data is transferred via a callback, this data will
be written into a queue.
* Queue contents then are taken in the main thread and further processed.


### MeshModel

MeshModel shall keep all data retrieved from the connected radio. These are
* Local node infos
* Local node configurations (config, module config)
* Remote node infos

The storage shall be based on standard Python data structures. Names shall correspond to what is 
used in the protobufs, so that new elements can be added easily and without copy-paste but by 
adding some configuration data resp. automatically by inspecting the data.

#### Actual data elements used

* my_info: device identification data
* deviceuiConfig: screen settings
* metadata: describes some capabilities of the local connected device
* channel: channel description and their keys
* config, moduleConfig: N slots describing each a distinct set of settings.
* node_info: describing properties of a node

The radio will only send certain fields for each data element. So, all fields might be missing. 
E.g. this is true for the node_info packets for remote nodes after a factory reset (no nodes 
are found so far) or when calling with `--no-nodes` parameter. Newer firmware releases will 
also add more fields. However, `nodenum` will always be available

Those variations shall be handled by the MeshModel itself.

All available fields are defined via the protobuf messages and can be retrieved from there.

#### Structure

Basically, the MeshModel is a list of nodes and the ID of the local connected node. 
All nodes shall behave exactly the same, independent if it is a local or remote node.

```plantuml
title MeshModel details

class MeshModel {
    localNode: int
    nodes: list
    getLocalNode()
    getNode(nodeNum)
}

class Node {
    isLocal: bool
    nodeNum: int
    node_info: NodeInfo
    my_info: dict
    metadata: dict
    deviceuiConfig: dict
    config: dict
    moduleConfig: dict
    channels: list[dict]
    getData(blockName)
    setData(blockName, dict)
    getField(blockName, fieldName)
    setField(blockName, fieldName, data)
}

class NodeInfo {
    nodeNum: int
    user: dict
    position: dict
    snr: float
    lastheard: int
    hops_away: int
    device_metrics: dict
    is_favorite: bool
    
}

MeshModel "1" *-- "1..N" Node
Node "1" *-- "1" NodeInfo

note right of Node
    //nodeNum// and //node_info// are always present. Other parts only if
    this node is a local node, otherwise None (//isLocal// as shortcut)
    //getData()// will return a complete dict of data, e.g. metadata
    //getField()// will return exactly one field of a block.
    If not present, the method will return None
end note

note right of NodeInfo
    at least the node info of the local node is present including
    //nodenum// and //user//
end note
```