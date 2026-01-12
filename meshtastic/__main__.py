""" Main Meshtastic
"""
import io
# We just hit the 1600 line limit for main.py, but I currently have a huge set of powermon/structured logging changes
# later we can have a separate changelist to refactor main.py into smaller files
# pylint: disable=R0917,C0302

from typing import List, Optional

import argparse

import logging
import platform
import sys
import time
from datetime import datetime
from io import TextIOWrapper

try:
    import pyqrcode  # type: ignore[import-untyped]
except ImportError as e:
    pyqrcode = None

import yaml
from google.protobuf.json_format import MessageToDict
from pubsub import pub  # type: ignore[import-untyped]

try:
    import meshtastic.test
    have_test = True
except ImportError as e:
    have_test = False

import meshtastic.util
from meshtastic.setup_arguments import initParser
from meshtastic.ble_interface import BLEInterface
from meshtastic.mesh_interface import MeshInterface
from meshtastic.command_factory import CommandFactory
from meshtastic.command_executor import CommandExecutor
from meshtastic.mesh_model import MeshModel
from meshtastic.protocol_manager import ProtocolHandlerManager
from meshtastic.topic_map import useNotifyByWriteFileEx

from meshtastic import BROADCAST_ADDR, mt_config, remote_hardware
try:
    from meshtastic.powermon import PowerMeter, PowerStress, PPK2PowerSupply, RidenPowerSupply, SimPowerSupply
    from meshtastic.slog import LogSet
    have_powermon = True
    powermon_exception = 'None'
    meter: Optional[PowerMeter] = None
except ImportError as e:
    have_powermon = False
    powermon_exception = e
    meter = None
from meshtastic.protobuf import channel_pb2, config_pb2, portnums_pb2, mesh_pb2

logger = logging.getLogger(__name__)

def onReceive(packet, interface) -> None:
    """Callback invoked when a packet arrives"""
    args = mt_config.args
    try:
        d = packet.get("decoded")
        logger.debug(f"in onReceive() d:{d}")

        # Exit once we receive a reply
        if (
            args
            and args.sendtext
            and packet["to"] == interface.myInfo.my_node_num
            and d.get("portnum", portnums_pb2.PortNum.UNKNOWN_APP) == portnums_pb2.PortNum.TEXT_MESSAGE_APP
        ):
            interface.close()  # after running command then exit

        # Reply to every received message with some stats
        if d is not None and args and args.reply:
            msg = d.get("text")
            if msg:
                rxSnr = packet["rxSnr"]
                hopLimit = packet["hopLimit"]
                print(f"message: {msg}")
                reply = f"got msg '{msg}' with rxSnr: {rxSnr} and hopLimit: {hopLimit}"
                print("Sending reply: ", reply)
                interface.sendText(reply)

    except Exception as ex:
        print(f"Warning: Error processing received packet: {ex}.")


def onConnection(interface, topic=pub.AUTO_TOPIC) -> None:  # pylint: disable=W0613
    """Callback invoked when we connect/disconnect from a radio"""
    print(f"Connection changed: {topic.getName()}")


def checkChannel(interface: MeshInterface, channelIndex: int) -> bool:
    """Given an interface and channel index, return True if that channel is non-disabled on the local node"""
    ch = interface.localNode.getChannelByChannelIndex(channelIndex)
    logger.debug(f"ch:{ch}")
    return ch and ch.role != channel_pb2.Channel.Role.DISABLED


def getPref(node, comp_name) -> bool:
    """Get a channel or preferences value"""
    def _printSetting(config_type, uni_name, pref_value, repeated):
        """Pretty print the setting"""
        if repeated:
            pref_value = [meshtastic.util.toStr(v) for v in pref_value]
        else:
            pref_value = meshtastic.util.toStr(pref_value)
        print(f"{str(config_type.name)}.{uni_name}: {str(pref_value)}")
        logger.debug(f"{str(config_type.name)}.{uni_name}: {str(pref_value)}")

    name = splitCompoundName(comp_name)
    wholeField = name[0] == name[1]  # We want the whole field

    camel_name = meshtastic.util.snake_to_camel(name[1])
    # Note: protobufs has the keys in snake_case, so snake internally
    snake_name = meshtastic.util.camel_to_snake(name[1])
    uni_name = camel_name if mt_config.camel_case else snake_name
    logger.debug(f"snake_name:{snake_name} camel_name:{camel_name}")
    logger.debug(f"use camel:{mt_config.camel_case}")

    # First validate the input
    localConfig = node.localConfig
    moduleConfig = node.moduleConfig
    found: bool = False
    for config in [localConfig, moduleConfig]:
        objDesc = config.DESCRIPTOR
        config_type = objDesc.fields_by_name.get(name[0])
        pref = ""		# FIXME - is this correct to leave as an empty string if not found?
        if config_type:
            pref = config_type.message_type.fields_by_name.get(snake_name)
            if pref or wholeField:
                found = True
                break

    if not found:
        print(
            f"{localConfig.__class__.__name__} and {moduleConfig.__class__.__name__} do not have attribute {uni_name}."
        )
        print("Choices are...")
        printConfig(localConfig)
        printConfig(moduleConfig)
        return False

    # Check if we need to request the config
    if len(config.ListFields()) != 0 and not isinstance(pref, str): # if str, it's still the empty string, I think
        # read the value
        config_values = getattr(config, config_type.name)
        if not wholeField:
            pref_value = getattr(config_values, pref.name)
            repeated = pref.label == pref.LABEL_REPEATED
            _printSetting(config_type, uni_name, pref_value, repeated)
        else:
            for field in config_values.ListFields():
                repeated = field[0].label == field[0].LABEL_REPEATED
                _printSetting(config_type, field[0].name, field[1], repeated)
    else:
        # Always show whole field for remote node
        node.requestConfig(config_type)

    return True


def splitCompoundName(comp_name: str) -> List[str]:
    """Split compound (dot separated) preference name into parts"""
    name: List[str] = comp_name.split(".")
    if len(name) < 2:
        name[0] = comp_name
        name.append(comp_name)
    return name


def traverseConfig(config_root, config, interface_config) -> bool:
    """Iterate through current config level preferences and either traverse deeper if preference is a dict or set preference"""
    snake_name = meshtastic.util.camel_to_snake(config_root)
    for pref in config:
        pref_name = f"{snake_name}.{pref}"
        if isinstance(config[pref], dict):
            traverseConfig(pref_name, config[pref], interface_config)
        else:
            setPref(interface_config, pref_name, config[pref])

    return True


def setPref(config, comp_name, raw_val) -> bool:
    """Set a channel or preferences value"""

    name = splitCompoundName(comp_name)

    snake_name = meshtastic.util.camel_to_snake(name[-1])
    camel_name = meshtastic.util.snake_to_camel(name[-1])
    uni_name = camel_name if mt_config.camel_case else snake_name
    logger.debug(f"snake_name:{snake_name}")
    logger.debug(f"camel_name:{camel_name}")

    objDesc = config.DESCRIPTOR
    config_part = config
    config_type = objDesc.fields_by_name.get(name[0])
    if config_type and config_type.message_type is not None:
        for name_part in name[1:-1]:
            part_snake_name = meshtastic.util.camel_to_snake((name_part))
            config_part = getattr(config, config_type.name)
            config_type = config_type.message_type.fields_by_name.get(part_snake_name)
    pref = None
    if config_type and config_type.message_type is not None:
        pref = config_type.message_type.fields_by_name.get(snake_name)
    # Others like ChannelSettings are standalone
    elif config_type:
        pref = config_type

    if (not pref) or (not config_type):
        return False

    if isinstance(raw_val, str):
        val = meshtastic.util.fromStr(raw_val)
    else:
        val = raw_val
    logger.debug(f"valStr:{raw_val} val:{val}")

    if snake_name == "wifi_psk" and len(str(raw_val)) < 8:
        print("Warning: network.wifi_psk must be 8 or more characters.")
        return False

    enumType = pref.enum_type
    # pylint: disable=C0123
    if enumType and type(val) == str:
        # We've failed so far to convert this string into an enum, try to find it by reflection
        e = enumType.values_by_name.get(val)
        if e:
            val = e.number
        else:
            print(
                f"{name[0]}.{uni_name} does not have an enum called {val}, so you can not set it."
            )
            print(f"Choices in sorted order are:")
            names = []
            for f in enumType.values:
                # Note: We must use the value of the enum (regardless if camel or snake case)
                names.append(f"{f.name}")
            for temp_name in sorted(names):
                print(f"    {temp_name}")
            return False

    # repeating fields need to be handled with append, not setattr
    if pref.label != pref.LABEL_REPEATED:
        try:
            if config_type.message_type is not None:
                config_values = getattr(config_part, config_type.name)
                setattr(config_values, pref.name, val)
            else:
                setattr(config_part, snake_name, val)
        except TypeError:
            # The setter didn't like our arg type guess try again as a string
            config_values = getattr(config_part, config_type.name)
            setattr(config_values, pref.name, str(val))
    elif type(val) == list:
        new_vals = [meshtastic.util.fromStr(x) for x in val]
        config_values = getattr(config, config_type.name)
        getattr(config_values, pref.name)[:] = new_vals
    else:
        config_values = getattr(config, config_type.name)
        if val == 0:
            # clear values
            print(f"Clearing {pref.name} list")
            del getattr(config_values, pref.name)[:]
        else:
            print(f"Adding '{raw_val}' to the {pref.name} list")
            cur_vals = [x for x in getattr(config_values, pref.name) if x not in [0, "", b""]]
            if val not in cur_vals:
                cur_vals.append(val)
            getattr(config_values, pref.name)[:] = cur_vals
        return True

    prefix = f"{'.'.join(name[0:-1])}." if config_type.message_type is not None else ""
    print(f"Set {prefix}{uni_name} to {raw_val}")

    return True


def onConnected(interface):
    """Callback invoked when we connect to a radio"""
    closeNow = False  # Should we drop the connection after we finish?
    waitForAckNak = (
        False  # Should we wait for an acknowledgment if we send to a remote node?
    )
    try:
        args = mt_config.args

        # convenient place to store any keyword args we pass to getNode
        getNode_kwargs = {
            "requestChannelAttempts": args.channel_fetch_attempts,
            "timeout": args.timeout
        }

        # do not print this line if we are exporting the config
        if not args.export_config:
            print("Connected to radio")

        if args.set_time is not None:
            interface.getNode(args.dest, False, **getNode_kwargs).setTime(args.set_time)

        if args.remove_position:
            closeNow = True
            waitForAckNak = True

            print("Removing fixed position and disabling fixed position setting")
            interface.getNode(args.dest, False, **getNode_kwargs).removeFixedPosition()
        elif args.setlat or args.setlon or args.setalt:
            closeNow = True
            waitForAckNak = True

            alt = 0
            lat = 0
            lon = 0
            if args.setalt:
                alt = int(args.setalt)
                print(f"Fixing altitude at {alt} meters")
            if args.setlat:
                try:
                    lat = int(args.setlat)
                except ValueError:
                    lat = float(args.setlat)
                print(f"Fixing latitude at {lat} degrees")
            if args.setlon:
                try:
                    lon = int(args.setlon)
                except ValueError:
                    lon = float(args.setlon)
                print(f"Fixing longitude at {lon} degrees")

            print("Setting device position and enabling fixed position setting")
            # can include lat/long/alt etc: latitude = 37.5, longitude = -122.1
            interface.getNode(args.dest, False, **getNode_kwargs).setFixedPosition(lat, lon, alt)

        if args.set_owner or args.set_owner_short or args.set_is_unmessageable:
            closeNow = True
            waitForAckNak = True

            long_name = args.set_owner.strip() if args.set_owner else None
            short_name = args.set_owner_short.strip() if args.set_owner_short else None

            if long_name is not None and not long_name:
                meshtastic.util.our_exit("ERROR: Long Name cannot be empty or contain only whitespace characters")

            if short_name is not None and not short_name:
                meshtastic.util.our_exit("ERROR: Short Name cannot be empty or contain only whitespace characters")

            if long_name and short_name:
                print(f"Setting device owner to {long_name} and short name to {short_name}")
            elif long_name:
                print(f"Setting device owner to {long_name}")
            elif short_name:
                print(f"Setting device owner short to {short_name}")

            unmessagable = None
            if args.set_is_unmessageable is not None:
                unmessagable = (
                    meshtastic.util.fromStr(args.set_is_unmessageable)
                    if isinstance(args.set_is_unmessageable, str)
                    else args.set_is_unmessageable
                )
                print(f"Setting device owner is_unmessageable to {unmessagable}")

            interface.getNode(args.dest, False, **getNode_kwargs).setOwner(
                long_name=long_name,
                short_name=short_name,
                is_unmessagable=unmessagable
            )

        if args.set_canned_message:
            closeNow = True
            waitForAckNak = True
            node = interface.getNode(args.dest, False, **getNode_kwargs)
            if node.module_available(mesh_pb2.CANNEDMSG_CONFIG):
                print(f"Setting canned plugin message to {args.set_canned_message}")
                node.set_canned_message(args.set_canned_message)
            else:
                print("Canned Message module is excluded by firmware; skipping set.")

        if args.set_ringtone:
            closeNow = True
            waitForAckNak = True
            node = interface.getNode(args.dest, False, **getNode_kwargs)
            if node.module_available(mesh_pb2.EXTNOTIF_CONFIG):
                print(f"Setting ringtone to {args.set_ringtone}")
                node.set_ringtone(args.set_ringtone)
            else:
                print("External Notification is excluded by firmware; skipping ringtone set.")

        if args.pos_fields:
            # If --pos-fields invoked with args, set position fields
            closeNow = True
            positionConfig = interface.getNode(args.dest, **getNode_kwargs).localConfig.position
            allFields = 0

            try:
                for field in args.pos_fields:
                    v_field = positionConfig.PositionFlags.Value(field)
                    allFields |= v_field

            except ValueError:
                print("ERROR: supported position fields are:")
                print(positionConfig.PositionFlags.keys())
                print(
                    "If no fields are specified, will read and display current value."
                )

            else:
                print(f"Setting position fields to {allFields}")
                setPref(positionConfig, "position_flags", f"{allFields:d}")
                print("Writing modified preferences to device")
                interface.getNode(args.dest, **getNode_kwargs).writeConfig("position")

        elif args.pos_fields is not None:
            # If --pos-fields invoked without args, read and display current value
            closeNow = True
            positionConfig = interface.getNode(args.dest, **getNode_kwargs).localConfig.position

            fieldNames = []
            for bit in positionConfig.PositionFlags.values():
                if positionConfig.position_flags & bit:
                    fieldNames.append(positionConfig.PositionFlags.Name(bit))
            print(" ".join(fieldNames))

        if args.set_ham:
            if not args.set_ham.strip():
                meshtastic.util.our_exit("ERROR: Ham radio callsign cannot be empty or contain only whitespace characters")
            closeNow = True
            print(f"Setting Ham ID to {args.set_ham} and turning off encryption")
            interface.getNode(args.dest, **getNode_kwargs).setOwner(args.set_ham, is_licensed=True)
            # Must turn off encryption on primary channel
            interface.getNode(args.dest, **getNode_kwargs).turnOffEncryptionOnPrimaryChannel()

        if args.reboot:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).reboot()

        if args.reboot_ota:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).rebootOTA()

        if args.enter_dfu:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).enterDFUMode()

        if args.shutdown:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).shutdown()

        if args.device_metadata:
            closeNow = True
            interface.getNode(args.dest, False, **getNode_kwargs).getMetadata()

        if args.begin_edit:
            closeNow = True
            interface.getNode(args.dest, False, **getNode_kwargs).beginSettingsTransaction()

        if args.commit_edit:
            closeNow = True
            interface.getNode(args.dest, False, **getNode_kwargs).commitSettingsTransaction()

        if args.factory_reset or args.factory_reset_device:
            closeNow = True
            waitForAckNak = True

            full = bool(args.factory_reset_device)
            interface.getNode(args.dest, False, **getNode_kwargs).factoryReset(full=full)

        if args.remove_node:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).removeNode(args.remove_node)

        if args.set_favorite_node:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).setFavorite(args.set_favorite_node)

        if args.remove_favorite_node:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).removeFavorite(args.remove_favorite_node)

        if args.set_ignored_node:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).setIgnored(args.set_ignored_node)

        if args.remove_ignored_node:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).removeIgnored(args.remove_ignored_node)

        if args.reset_nodedb:
            closeNow = True
            waitForAckNak = True
            interface.getNode(args.dest, False, **getNode_kwargs).resetNodeDb()

        if args.sendtext:
            closeNow = True
            channelIndex = mt_config.channel_index or 0
            if checkChannel(interface, channelIndex):
                print(
                    f"Sending text message {args.sendtext} to {args.dest} on channelIndex:{channelIndex}"
                    f" {'using PRIVATE_APP port' if args.private else ''}"
                )
                interface.sendText(
                    args.sendtext,
                    args.dest,
                    wantAck=True,
                    channelIndex=channelIndex,
                    onResponse=interface.getNode(args.dest, False, **getNode_kwargs).onAckNak,
                    portNum=portnums_pb2.PortNum.PRIVATE_APP if args.private else portnums_pb2.PortNum.TEXT_MESSAGE_APP
                )
            else:
                meshtastic.util.our_exit(
                    f"Warning: {channelIndex} is not a valid channel. Channel must not be DISABLED."
                )

        if args.traceroute:
            loraConfig = getattr(interface.localNode.localConfig, "lora")
            hopLimit = getattr(loraConfig, "hop_limit")
            dest = str(args.traceroute)
            channelIndex = mt_config.channel_index or 0
            if checkChannel(interface, channelIndex):
                print(
                    f"Sending traceroute request to {dest} on channelIndex:{channelIndex} (this could take a while)"
                )
                interface.sendTraceRoute(dest, hopLimit, channelIndex=channelIndex)

        if args.request_telemetry:
            if args.dest == BROADCAST_ADDR:
                meshtastic.util.our_exit("Warning: Must use a destination node ID.")
            else:
                channelIndex = mt_config.channel_index or 0
                if checkChannel(interface, channelIndex):
                    telemMap = {
                        "device": "device_metrics",
                        "environment": "environment_metrics",
                        "air_quality": "air_quality_metrics",
                        "airquality": "air_quality_metrics",
                        "power": "power_metrics",
                        "localstats": "local_stats",
                        "local_stats": "local_stats",
                    }
                    telemType = telemMap.get(args.request_telemetry, "device_metrics")
                    print(
                        f"Sending {telemType} telemetry request to {args.dest} on channelIndex:{channelIndex} (this could take a while)"
                    )
                    interface.sendTelemetry(
                        destinationId=args.dest,
                        wantResponse=True,
                        channelIndex=channelIndex,
                        telemetryType=telemType,
                    )

        if args.request_position:
            if args.dest == BROADCAST_ADDR:
                meshtastic.util.our_exit("Warning: Must use a destination node ID.")
            else:
                channelIndex = mt_config.channel_index or 0
                if checkChannel(interface, channelIndex):
                    print(
                        f"Sending position request to {args.dest} on channelIndex:{channelIndex} (this could take a while)"
                    )
                    interface.sendPosition(
                        destinationId=args.dest,
                        wantResponse=True,
                        channelIndex=channelIndex,
                    )

        if args.gpio_wrb or args.gpio_rd or args.gpio_watch:
            if args.dest == BROADCAST_ADDR:
                meshtastic.util.our_exit("Warning: Must use a destination node ID.")
            else:
                rhc = remote_hardware.RemoteHardwareClient(interface)

                if args.gpio_wrb:
                    bitmask = 0
                    bitval = 0
                    for wrpair in args.gpio_wrb or []:
                        bitmask |= 1 << int(wrpair[0])
                        bitval |= int(wrpair[1]) << int(wrpair[0])
                    print(
                        f"Writing GPIO mask 0x{bitmask:x} with value 0x{bitval:x} to {args.dest}"
                    )
                    rhc.writeGPIOs(args.dest, bitmask, bitval)
                    closeNow = True

                if args.gpio_rd:
                    bitmask = int(args.gpio_rd, 16)
                    print(f"Reading GPIO mask 0x{bitmask:x} from {args.dest}")
                    interface.mask = bitmask
                    rhc.readGPIOs(args.dest, bitmask, None)
                    # wait up to X seconds for a response
                    for _ in range(10):
                        time.sleep(1)
                        if interface.gotResponse:
                            break
                    logger.debug(f"end of gpio_rd")

                if args.gpio_watch:
                    bitmask = int(args.gpio_watch, 16)
                    print(
                        f"Watching GPIO mask 0x{bitmask:x} from {args.dest}. Press ctrl-c to exit"
                    )
                    while True:
                        rhc.watchGPIOs(args.dest, bitmask)
                        time.sleep(1)

        # handle settings
        if args.set:
            closeNow = True
            waitForAckNak = True
            node = interface.getNode(args.dest, False, **getNode_kwargs)

            # Handle the int/float/bool arguments
            pref = None
            fields = set()
            for pref in args.set:
                found = False
                field = splitCompoundName(pref[0].lower())[0]
                for config in [node.localConfig, node.moduleConfig]:
                    config_type = config.DESCRIPTOR.fields_by_name.get(field)
                    if config_type:
                        if len(config.ListFields()) == 0:
                            node.requestConfig(
                                config.DESCRIPTOR.fields_by_name.get(field)
                            )
                        found = setPref(config, pref[0], pref[1])
                        if found:
                            fields.add(field)
                            break

            if found:
                print("Writing modified preferences to device")
                if len(fields) > 1:
                    print("Using a configuration transaction")
                    node.beginSettingsTransaction()
                for field in fields:
                    print(f"Writing {field} configuration to device")
                    node.writeConfig(field)
                if len(fields) > 1:
                    node.commitSettingsTransaction()
            else:
                if mt_config.camel_case:
                    print(
                        f"{node.localConfig.__class__.__name__} and {node.moduleConfig.__class__.__name__} do not have an attribute {pref[0]}."
                    )
                else:
                    print(
                        f"{node.localConfig.__class__.__name__} and {node.moduleConfig.__class__.__name__} do not have attribute {pref[0]}."
                    )
                print("Choices are...")
                printConfig(node.localConfig)
                printConfig(node.moduleConfig)

        if args.configure:
            with open(args.configure[0], encoding="utf8") as file:
                configuration = yaml.safe_load(file)
                closeNow = True

                interface.getNode(args.dest, False, **getNode_kwargs).beginSettingsTransaction()

                if "owner" in configuration:
                    # Validate owner name before setting
                    owner_name = str(configuration["owner"]).strip()
                    if not owner_name:
                        meshtastic.util.our_exit("ERROR: Long Name cannot be empty or contain only whitespace characters")
                    print(f"Setting device owner to {configuration['owner']}")
                    waitForAckNak = True
                    interface.getNode(args.dest, False, **getNode_kwargs).setOwner(configuration["owner"])
                    time.sleep(0.5)

                if "owner_short" in configuration:
                    # Validate owner short name before setting
                    owner_short_name = str(configuration["owner_short"]).strip()
                    if not owner_short_name:
                        meshtastic.util.our_exit("ERROR: Short Name cannot be empty or contain only whitespace characters")
                    print(
                        f"Setting device owner short to {configuration['owner_short']}"
                    )
                    waitForAckNak = True
                    interface.getNode(args.dest, False, **getNode_kwargs).setOwner(
                        long_name=None, short_name=configuration["owner_short"]
                    )
                    time.sleep(0.5)

                if "ownerShort" in configuration:
                    # Validate owner short name before setting
                    owner_short_name = str(configuration["ownerShort"]).strip()
                    if not owner_short_name:
                        meshtastic.util.our_exit("ERROR: Short Name cannot be empty or contain only whitespace characters")
                    print(
                        f"Setting device owner short to {configuration['ownerShort']}"
                    )
                    waitForAckNak = True
                    interface.getNode(args.dest, False, **getNode_kwargs).setOwner(
                        long_name=None, short_name=configuration["ownerShort"]
                    )
                    time.sleep(0.5)

                if "channel_url" in configuration:
                    print("Setting channel url to", configuration["channel_url"])
                    interface.getNode(args.dest, **getNode_kwargs).setURL(configuration["channel_url"])
                    time.sleep(0.5)

                if "channelUrl" in configuration:
                    print("Setting channel url to", configuration["channelUrl"])
                    interface.getNode(args.dest, **getNode_kwargs).setURL(configuration["channelUrl"])
                    time.sleep(0.5)

                if "canned_messages" in configuration:
                    print("Setting canned message messages to", configuration["canned_messages"])
                    interface.getNode(args.dest, **getNode_kwargs).set_canned_message(configuration["canned_messages"])
                    time.sleep(0.5)

                if "ringtone" in configuration:
                    print("Setting ringtone to", configuration["ringtone"])
                    interface.getNode(args.dest, **getNode_kwargs).set_ringtone(configuration["ringtone"])
                    time.sleep(0.5)

                if "location" in configuration:
                    alt = 0
                    lat = 0.0
                    lon = 0.0
                    localConfig = interface.localNode.localConfig

                    if "alt" in configuration["location"]:
                        alt = int(configuration["location"]["alt"] or 0)
                        print(f"Fixing altitude at {alt} meters")
                    if "lat" in configuration["location"]:
                        lat = float(configuration["location"]["lat"] or 0)
                        print(f"Fixing latitude at {lat} degrees")
                    if "lon" in configuration["location"]:
                        lon = float(configuration["location"]["lon"] or 0)
                        print(f"Fixing longitude at {lon} degrees")
                    print("Setting device position")
                    interface.localNode.setFixedPosition(lat, lon, alt)
                    time.sleep(0.5)

                if "config" in configuration:
                    localConfig = interface.getNode(args.dest, **getNode_kwargs).localConfig
                    for section in configuration["config"]:
                        traverseConfig(
                            section, configuration["config"][section], localConfig
                        )
                        interface.getNode(args.dest, **getNode_kwargs).writeConfig(
                            meshtastic.util.camel_to_snake(section)
                        )
                        time.sleep(0.5)

                if "module_config" in configuration:
                    moduleConfig = interface.getNode(args.dest, **getNode_kwargs).moduleConfig
                    for section in configuration["module_config"]:
                        traverseConfig(
                            section,
                            configuration["module_config"][section],
                            moduleConfig,
                        )
                        interface.getNode(args.dest, **getNode_kwargs).writeConfig(
                            meshtastic.util.camel_to_snake(section)
                        )
                        time.sleep(0.5)

                interface.getNode(args.dest, False, **getNode_kwargs).commitSettingsTransaction()
                print("Writing modified configuration to device")

        if args.ch_set_url:
            closeNow = True
            interface.getNode(args.dest, **getNode_kwargs).setURL(args.ch_set_url, addOnly=False)

        # handle changing channels

        if args.ch_add_url:
            closeNow = True
            interface.getNode(args.dest, **getNode_kwargs).setURL(args.ch_add_url, addOnly=True)

        if args.ch_add:
            channelIndex = mt_config.channel_index
            if channelIndex is not None:
                # Since we set the channel index after adding a channel, don't allow --ch-index
                meshtastic.util.our_exit(
                    "Warning: '--ch-add' and '--ch-index' are incompatible. Channel not added."
                )
            closeNow = True
            if len(args.ch_add) > 10:
                meshtastic.util.our_exit(
                    "Warning: Channel name must be shorter. Channel not added."
                )
            n = interface.getNode(args.dest, **getNode_kwargs)
            ch = n.getChannelByName(args.ch_add)
            if ch:
                meshtastic.util.our_exit(
                    f"Warning: This node already has a '{args.ch_add}' channel. No changes were made."
                )
            else:
                # get the first channel that is disabled (i.e., available)
                ch = n.getDisabledChannel()
                if not ch:
                    meshtastic.util.our_exit("Warning: No free channels were found")
                chs = channel_pb2.ChannelSettings()
                chs.psk = meshtastic.util.genPSK256()
                chs.name = args.ch_add
                ch.settings.CopyFrom(chs)
                ch.role = channel_pb2.Channel.Role.SECONDARY
                print(f"Writing modified channels to device")
                n.writeChannel(ch.index)
                if channelIndex is None:
                    print(
                        f"Setting newly-added channel's {ch.index} as '--ch-index' for further modifications"
                    )
                    mt_config.channel_index = ch.index

        if args.ch_del:
            closeNow = True

            channelIndex = mt_config.channel_index
            if channelIndex is None:
                meshtastic.util.our_exit(
                    "Warning: Need to specify '--ch-index' for '--ch-del'.", 1
                )
            else:
                if channelIndex == 0:
                    meshtastic.util.our_exit(
                        "Warning: Cannot delete primary channel.", 1
                    )
                else:
                    print(f"Deleting channel {channelIndex}")
                    ch = interface.getNode(args.dest, **getNode_kwargs).deleteChannel(channelIndex)

        def setSimpleConfig(modem_preset):
            """Set one of the simple modem_config"""
            channelIndex = mt_config.channel_index
            if channelIndex is not None and channelIndex > 0:
                meshtastic.util.our_exit(
                    "Warning: Cannot set modem preset for non-primary channel", 1
                )
            # Overwrite modem_preset
            node = interface.getNode(args.dest, False, **getNode_kwargs)
            if len(node.localConfig.ListFields()) == 0:
                node.requestConfig(node.localConfig.DESCRIPTOR.fields_by_name.get("lora"))
            node.localConfig.lora.modem_preset = modem_preset
            node.writeConfig("lora")

        # handle the simple radio set commands
        if args.ch_vlongslow:
            setSimpleConfig(config_pb2.Config.LoRaConfig.ModemPreset.VERY_LONG_SLOW)

        if args.ch_longslow:
            setSimpleConfig(config_pb2.Config.LoRaConfig.ModemPreset.LONG_SLOW)

        if args.ch_longfast:
            setSimpleConfig(config_pb2.Config.LoRaConfig.ModemPreset.LONG_FAST)

        if args.ch_medslow:
            setSimpleConfig(config_pb2.Config.LoRaConfig.ModemPreset.MEDIUM_SLOW)

        if args.ch_medfast:
            setSimpleConfig(config_pb2.Config.LoRaConfig.ModemPreset.MEDIUM_FAST)

        if args.ch_shortslow:
            setSimpleConfig(config_pb2.Config.LoRaConfig.ModemPreset.SHORT_SLOW)

        if args.ch_shortfast:
            setSimpleConfig(config_pb2.Config.LoRaConfig.ModemPreset.SHORT_FAST)

        if args.ch_set or args.ch_enable or args.ch_disable:
            closeNow = True

            channelIndex = mt_config.channel_index
            if channelIndex is None:
                meshtastic.util.our_exit("Warning: Need to specify '--ch-index'.", 1)
            node = interface.getNode(args.dest, **getNode_kwargs)
            ch = node.channels[channelIndex]

            if args.ch_enable or args.ch_disable:
                print(
                    "Warning: --ch-enable and --ch-disable can produce noncontiguous channels, "
                    "which can cause errors in some clients. Whenever possible, use --ch-add and --ch-del instead."
                )
                if channelIndex == 0:
                    meshtastic.util.our_exit(
                        "Warning: Cannot enable/disable PRIMARY channel."
                    )

                enable = True  # default to enable
                if args.ch_enable:
                    enable = True
                if args.ch_disable:
                    enable = False

            # Handle the channel settings
            for pref in args.ch_set or []:
                if pref[0] == "psk":
                    found = True
                    ch.settings.psk = meshtastic.util.fromPSK(pref[1])
                else:
                    found = setPref(ch.settings, pref[0], pref[1])
                if not found:
                    category_settings = ["module_settings"]
                    print(
                        f"{ch.settings.__class__.__name__} does not have an attribute {pref[0]}."
                    )
                    print("Choices are...")
                    for field in ch.settings.DESCRIPTOR.fields:
                        if field.name not in category_settings:
                            print(f"{field.name}")
                        else:
                            print(f"{field.name}:")
                            config = ch.settings.DESCRIPTOR.fields_by_name.get(
                                field.name
                            )
                            names = []
                            for sub_field in config.message_type.fields:
                                tmp_name = f"{field.name}.{sub_field.name}"
                                names.append(tmp_name)
                            for temp_name in sorted(names):
                                print(f"    {temp_name}")

                enable = True  # If we set any pref, assume the user wants to enable the channel

            if enable:
                ch.role = (
                    channel_pb2.Channel.Role.PRIMARY
                    if (channelIndex == 0)
                    else channel_pb2.Channel.Role.SECONDARY
                )
            else:
                ch.role = channel_pb2.Channel.Role.DISABLED

            print(f"Writing modified channels to device")
            node.writeChannel(channelIndex)

        if args.get_canned_message:
            closeNow = True
            print("")
            messages = interface.getNode(args.dest, **getNode_kwargs).get_canned_message()
            print(f"canned_plugin_message:{messages}")

        if args.get_ringtone:
            closeNow = True
            print("")
            ringtone = interface.getNode(args.dest, **getNode_kwargs).get_ringtone()
            print(f"ringtone:{ringtone}")

        if args.info:
            print("")
            # If we aren't trying to talk to our local node, don't show it
            if args.dest == BROADCAST_ADDR:
                interface.showInfo()
                print("")
                interface.getNode(args.dest, **getNode_kwargs).showInfo()
                closeNow = True
                print("")
                pypi_version = meshtastic.util.check_if_newer_version()
                if pypi_version:
                    print(
                        f"*** A newer version v{pypi_version} is available!"
                        ' Consider running "pip install --upgrade meshtastic" ***\n'
                    )
            else:
                print("Showing info of remote node is not supported.")
                print(
                    "Use the '--get' command for a specific configuration (e.g. 'lora') instead."
                )

        if args.get:
            closeNow = True
            node = interface.getNode(args.dest, False, **getNode_kwargs)
            for pref in args.get:
                found = getPref(node, pref[0])

            if found:
                print("Completed getting preferences")

        if args.nodes:
            closeNow = True
            if args.dest != BROADCAST_ADDR:
                print("Showing node list of a remote node is not supported.")
                return
            interface.showNodes(True, args.show_fields)

        if args.show_fields and not args.nodes:
            print("--show-fields can only be used with --nodes")
            return

        if args.qr or args.qr_all:
            closeNow = True
            url = interface.getNode(args.dest, True, **getNode_kwargs).getURL(includeAll=args.qr_all)
            if args.qr_all:
                urldesc = "Complete URL (includes all channels)"
            else:
                urldesc = "Primary channel URL"
            print(f"{urldesc}: {url}")
            if pyqrcode is not None:
                qr = pyqrcode.create(url)
                print(qr.terminal())
            else:
                print("Install pyqrcode to view a QR code printed to terminal.")

        log_set: Optional = None  # type: ignore[annotation-unchecked]
        # we need to keep a reference to the logset so it doesn't get GCed early

        if args.slog or args.power_stress:
            if have_powermon:
                # Setup loggers
                global meter  # pylint: disable=global-variable-not-assigned
                log_set = LogSet(
                    interface, args.slog if args.slog != "default" else None, meter
                )

                if args.power_stress:
                    stress = PowerStress(interface)
                    stress.run()
                    closeNow = True  # exit immediately after stress test
            else:
                meshtastic.util.our_exit("The powermon module could not be loaded. "
                                         "You may need to run `poetry install --with powermon`. "
                                         "Import Error was: " + powermon_exception)

        if args.listen:
            closeNow = False

        have_tunnel = platform.system() == "Linux"
        if have_tunnel and args.tunnel:
            if args.dest != BROADCAST_ADDR:
                print("A tunnel can only be created using the local node.")
                return
            # pylint: disable=C0415
            from . import tunnel

            # Even if others said we could close, stay open if the user asked for a tunnel
            closeNow = False
            if interface.noProto:
                logger.warning(f"Not starting Tunnel - disabled by noProto")
            else:
                if args.tunnel_net:
                    tunnel.Tunnel(interface, subnet=args.tunnel_net)
                else:
                    tunnel.Tunnel(interface)

        if args.ack or (args.dest != BROADCAST_ADDR and waitForAckNak):
            print(
                f"Waiting for an acknowledgment from remote node (this could take a while)"
            )
            interface.getNode(args.dest, False, **getNode_kwargs).iface.waitForAckNak()

        if args.wait_to_disconnect:
            print(f"Waiting {args.wait_to_disconnect} seconds before disconnecting")
            time.sleep(int(args.wait_to_disconnect))

        # if the user didn't ask for serial debugging output, we might want to exit after we've done our operation
        if (not args.seriallog) and closeNow:
            interface.close()  # after running command then exit

        # Close any structured logs after we've done all of our API operations
        if log_set:
            log_set.close()

    except Exception as ex:
        print(f"Aborting due to: {ex}")
        interface.close()  # close the connection now, so that our app exits
        sys.exit(1)


def printConfig(config) -> None:
    """print configuration"""
    objDesc = config.DESCRIPTOR
    for config_section in objDesc.fields:
        if config_section.name != "version":
            config = objDesc.fields_by_name.get(config_section.name)
            print(f"{config_section.name}:")
            names = []
            for field in config.message_type.fields:
                tmp_name = f"{config_section.name}.{field.name}"
                if mt_config.camel_case:
                    tmp_name = meshtastic.util.snake_to_camel(tmp_name)
                names.append(tmp_name)
            for temp_name in sorted(names):
                print(f"    {temp_name}")


def onNode(node) -> None:
    """Callback invoked when the node DB changes"""
    print(f"Node changed: {node}")


def subscribe() -> None:
    """Subscribe to the topics the user probably wants to see, prints output to stdout"""
    pub.subscribe(onReceive, "meshtastic.receive")
    # pub.subscribe(onConnection, "meshtastic.connection")

    # We now call onConnected from main
    # pub.subscribe(onConnected, "meshtastic.connection.established")

    # pub.subscribe(onNode, "meshtastic.node")

def create_power_meter():
    """Set up the power meter."""

    global meter  # pylint: disable=global-statement
    args = mt_config.args

    # If the user specified a voltage, make sure it is valid
    v = 0.0
    if args.power_voltage:
        v = float(args.power_voltage)
        if v < 0.8 or v > 5.0:
            meshtastic.util.our_exit("Voltage must be between 0.8 and 5.0")

    if args.power_riden:
        meter = RidenPowerSupply(args.power_riden)
    elif args.power_ppk2_supply or args.power_ppk2_meter:
        meter = PPK2PowerSupply()
        assert v > 0, "Voltage must be specified for PPK2"
        meter.v = v  # PPK2 requires setting voltage before selecting supply mode
        meter.setIsSupply(args.power_ppk2_supply)
    elif args.power_sim:
        meter = SimPowerSupply()

    if meter and v:
        logger.info(f"Setting power supply to {v} volts")
        meter.v = v
        meter.powerOn()

        if args.power_wait:
            input("Powered on, press enter to continue...")
        else:
            logger.info("Powered-on, waiting for device to boot")
            time.sleep(5)


def common():
    """Shared code for all of our command line wrappers."""
    args = mt_config.args
    parser = mt_config.parser
    logging.basicConfig(
        level=logging.DEBUG if (args.debug or args.listen) else logging.INFO,
        format="%(levelname)s file:%(filename)s %(funcName)s line:%(lineno)s [%(threadName)s] %(message)s",
    )

    # set all meshtastic loggers to DEBUG
    if not (args.debug or args.listen) and args.debuglib:
        logging.getLogger('meshtastic').setLevel(logging.DEBUG)

    # Execute all options where we will return fast and without connection
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        meshtastic.util.our_exit("", 1)
    if args.support:
        meshtastic.util.support_info()
        meshtastic.util.our_exit("", 0)
    if args.deprecated is not None:
        logger.error("This option has been deprecated, see help below for the correct replacement...")
        parser.print_help(sys.stderr)
        meshtastic.util.our_exit("", 1)
    if args.ble_scan:
        # Only scan for nodes, do not connect with this option
        logger.debug("BLE scan starting")
        for x in BLEInterface.scan():
            print(f"Found: name='{x.name}' address='{x.address}'")
        meshtastic.util.our_exit("BLE scan finished", 0)

    mt_config.serialLogfile = configureSerialLog(args.seriallog, args.noproto)
    mt_config.pubsubLogfile = configurePubSubLogging(args.debug)

    if have_powermon:
        create_power_meter()

    if args.test:
        if not have_test:
            meshtastic.util.our_exit("Test module could not be imported. Ensure you have the 'dotmap' module installed.")

        result = meshtastic.test.testAll()
        if not result:
            meshtastic.util.our_exit("Warning: Test was not successful.")
        else:
            meshtastic.util.our_exit("Test was a success.", 0)

    # # Early validation for owner names before attempting device connection
    # if hasattr(args, 'set_owner') and args.set_owner is not None:
    #     stripped_long_name = args.set_owner.strip()
    #     if not stripped_long_name:
    #         meshtastic.util.our_exit("ERROR: Long Name cannot be empty or contain only whitespace characters")
    #
    # if hasattr(args, 'set_owner_short') and args.set_owner_short is not None:
    #     stripped_short_name = args.set_owner_short.strip()
    #     if not stripped_short_name:
    #         meshtastic.util.our_exit("ERROR: Short Name cannot be empty or contain only whitespace characters")
    #
    # if hasattr(args, 'set_ham') and args.set_ham is not None:
    #     stripped_ham_name = args.set_ham.strip()
    #     if not stripped_ham_name:
    #         meshtastic.util.our_exit("ERROR: Ham radio callsign cannot be empty or contain only whitespace characters")
    #

    # subscribe()
    ifceType = {
        "ble": args.ble,
        "host": args.host,
        "serial": args.port,
        "noProto": args.noproto,
    }
    ifceArgs = {
        "timeout": args.timeout
    }
    phArgs = {
        "debugOut": mt_config.serialLogfile,
        "noNodes": args.no_nodes,
    }
    argDict = vars(args)
    argsKeys = list(argDict.keys())
    cmdList = CommandFactory(argsKeys).createCommandList(vars(args))
    client = MeshInterface(ifceType, **ifceArgs)
    with ProtocolHandlerManager(client) as pm:
        pm.instantiateHandlers(**phArgs)
        mesh = MeshModel()
        CommandExecutor(pm, mesh, args.timeout).execute(cmdList)

    # shutdown client correctly
    if client.isConnected:
        client.close()

    # We assume client is fully connected now
    # onConnected(client)

    have_tunnel = platform.system() == "Linux"
    if (
        args.noproto
        or args.reply
        or (have_tunnel and args.tunnel)
        or args.listen
    ):  # loop until someone presses ctrlc
        try:
            while True:
                time.sleep(1000)
        except KeyboardInterrupt:
            logger.info("Exiting due to keyboard interrupt")

    # don't call exit, background threads might be running still
    # sys.exit(0)


def configureSerialLog(serLogCfg: str | None, cfgNoproto: bool) -> io.TextIOWrapper | None:
    """setup file for serial log if configured so"""
    logfile = None
    if serLogCfg:
        if serLogCfg == "stdout":
            logfile = sys.stdout
        elif len(serLogCfg) > 0:
            # Note: using "line buffering"
            # pylint: disable=R1732
            logfile = open(serLogCfg, "w+", buffering=1, encoding="utf8")
    else:
        if cfgNoproto:
            logfile = sys.stdout

    if logfile is None:
        logger.debug("Not logging serial output")
    else:
        logger.info(f"Logging serial output to {logfile.name}")
    return logfile


def configurePubSubLogging(cfgDebug: bool) -> io.TextIOWrapper | None:
    """setup logging for pub/sub: just open distinct file"""
    logfile = None
    if cfgDebug:
        logfile = open('pubsub.log', 'w+', buffering=1, encoding="utf8")
        useNotifyByWriteFileEx(logfile)
    return logfile


def main():
    """Perform command line meshtastic operations"""
    try:
        parser = argparse.ArgumentParser(
            add_help=False,
            epilog="If no connection arguments are specified, we search for a compatible serial device, "
            "and if none is found, then attempt a TCP connection to localhost.",
        )
        mt_config.parser = parser
        initParser()
        common()
    finally:
        if lf := mt_config.serialLogfile:
            lf.close()
        if lf := mt_config.pubsubLogfile:
            lf.close()


def tunnelMain():
    """Run a meshtastic IP tunnel"""
    parser = argparse.ArgumentParser(add_help=False)
    mt_config.parser = parser
    initParser()
    args = mt_config.args
    args.tunnel = True
    mt_config.args = args
    common()


if __name__ == "__main__":
    main()
