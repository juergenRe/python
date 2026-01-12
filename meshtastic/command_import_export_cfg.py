"""Execution of 'export-config' and 'configure' commands"""
import base64
import logging
import yaml
from pathlib import Path
from typing import Any

from pubsub import pub  # type: ignore[import-untyped]

from google.protobuf.json_format import ParseDict

from meshtastic.protobuf import apponly_pb2

from meshtastic import BROADCAST_ADDR, LOCAL_ADDR, BROADCAST_NUM, mt_config
from meshtastic.mesh_model import MeshModel, Node
from meshtastic.command_interface import CmdError
from meshtastic.commands import Command, FIELD_MYINFO
from meshtastic.util import pskToString, snake_to_camel

logger = logging.getLogger(__name__)


class ExportConfigCommand(Command):
    """Defines the export-config command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, model: MeshModel, timeout: int) -> tuple:
        """Show human-readable summary about this object"""
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}. Print to: {self.parameter}")
        if self.destinationNode != BROADCAST_ADDR:
            return CmdError.ERROR, "Exporting configuration of remote nodes is not supported."
        yamlTxt = self.exportConfig(model.getLocalNode())
        outFile = self.parameter[0]
        if outFile == '-':
            print(yamlTxt)
        else:
            try:
                Path(outFile).write_text(yamlTxt, encoding="utf-8")
            except Exception as e:
                logger.debug(f"ERROR: Failed to write config file: {e}")
                return CmdError.ERROR, f"Failed to write config file: {e}"
        return CmdError.OK, f"Exported configuration to {outFile}"

    def setMissingFlagsFalse(self, cfg: dict, true_defaults: set[tuple[str, str]]) -> None:
        """Ensure that missing default=True keys are present in the cfg dictionary and set to False."""
        for setting in true_defaults:
            d = cfg
            for key in setting[:-1]:
                if key not in d or not isinstance(d[key], dict):
                    d[key] = {}
                d = d[key]
            if setting[-1] not in d:
                d[setting[-1]] = False

    def exportConfig(self, localNode: Node) -> str:
        """used in --export-config command"""
        configObj = {}

        # A list of configuration keys that should be set to False if they are missing
        configTrueDefaults = {
            ("bluetooth", "enabled"),
            ("lora", "sx126xRxBoostedGain"),
            ("lora", "txEnabled"),
            ("lora", "usePreset"),
            ("position", "positionBroadcastSmartEnabled"),
            ("security", "serialEnabled"),
        }

        moduleTrueDefaults = {
            ("mqtt", "encryptionEnabled"),
        }

        longName, shortName = localNode.getName(default=None)
        if longName:
            configObj["owner"] = longName
        if shortName:
            configObj["owner_short"] = shortName

        channelUrl = localNode.getUrl()
        if channelUrl:
            if mt_config.camel_case:
                configObj["channelUrl"] = channelUrl
            else:
                configObj["channel_url"] = channelUrl

        # treat position info
        pos = localNode.getDataElement('myInfo', 'position', default={'latitude': None, 'longitude': None, 'altitude': None})
        lat = pos.get("latitude")
        lon = pos.get("longitude")
        alt = pos.get("altitude")
        # lat and lon don't make much sense without the other (so fill with 0s), and alt isn't meaningful without both
        if lat or lon:
            configObj["location"] = {"lat": lat or float(0), "lon": lon or float(0)}
            if alt:
                configObj["location"]["alt"] = alt

        # canned_messages = interface.getCannedMessage()
        # ringtone = interface.getRingtone()
        # if canned_messages:
        #     configObj["canned_messages"] = canned_messages
        # if ringtone:
        #     configObj["ringtone"] = ringtone

        config = localNode.getField('config')
        if config:
            # Convert inner keys to correct snake/camelCase
            prefs = {}
            for pref in config:
                if mt_config.camel_case:
                    prefs[snake_to_camel(pref)] = config[pref]
                else:
                    prefs[pref] = config[pref]
                # mark base64 encoded fields as such
                if pref == "security":
                    if 'privateKey' in prefs[pref]:
                        prefs[pref]['privateKey'] = 'base64:' + prefs[pref]['privateKey']
                    if 'publicKey' in prefs[pref]:
                        prefs[pref]['publicKey'] = 'base64:' + prefs[pref]['publicKey']
                    if 'adminKey' in prefs[pref]:
                        for i in range(len(prefs[pref]['adminKey'])):
                            prefs[pref]['adminKey'][i] = 'base64:' + prefs[pref]['adminKey'][i]

            self.setMissingFlagsFalse(prefs, configTrueDefaults)
            configObj['config'] = prefs

        moduleConfig = localNode.getField('moduleConfig')
        if moduleConfig:
            # Convert inner keys to correct snake/camelCase
            prefs = {}
            for pref in moduleConfig:
                if len(moduleConfig[pref]) > 0:
                    prefs[pref] = moduleConfig[pref]
            self.setMissingFlagsFalse(prefs, moduleTrueDefaults)
            configObj["module_config"] = prefs

        return f"# start of Meshtastic configure yaml\n{yaml.dump(configObj)}"


class ConfigureCommand(Command):
    """Defines the configure command"""
    def __init__(self, destNode: str | None, chIndex: int | None, parameter: list = ()):
        super().__init__(destNode, chIndex, parameter)

    def execute(self, model: MeshModel, timeout: int) -> tuple:
        """Show human-readable summary about this object"""
        logger.debug(f"Execute {self.cmdName} {self.destinationNode}")
        return CmdError.OK, ""
