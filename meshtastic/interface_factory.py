"""Factory to create the specific connection to the radio according to the arguments passed"""

from typing import Union

import logging

from meshtastic.mesh_interface import InterfaceOpenError
from meshtastic.serial_interface import SerialInterface
from meshtastic.tcp_interface import TCPInterface, DEFAULT_TCP_PORT
from meshtastic.ble_interface import BLEInterface
import meshtastic.util

logger = logging.getLogger(__name__)

class InterfaceFactory:
    """Factory to create the specific connection to the radio according to the arguments passed"""
    def __init__(self):
        """set up factory"""
        self.interfaces = {
            "serial": SerialInterface,
            "tcp": TCPInterface,
            "ble": BLEInterface
        }

    def selectInterfaceType(self, ble, host, port) -> tuple:
        """return the interface class and the address parameter according to the arguments supplied
        during the call of the CLI"""
        if ble:
            param = ble if ble != "any" else None
            return self.interfaces["ble"], param
        if host:
            return self.interfaces["tcp"], host
        if port:
            return self.interfaces["serial"], port
        return None, ''

    def createInterface(self,
                        ble: str = None,
                        host: str = None,
                        serial: str = None,
                        **kwargs) -> Union[SerialInterface, TCPInterface, BLEInterface, None]:
        """create an instance of the needed interface acording passed kwargs"""
        if host:
            if ":" not in host:
                host = f"{host}:{DEFAULT_TCP_PORT}"

        kwargs["connectNow"] = True

        ifceClass, param = self.selectInterfaceType(ble, host, serial)
        if ifceClass is None:
            return None

        try:
            # connect to interface with standard signature:
            # address, connectNow, timeout, noNodes, debugOut, noProto
            client = ifceClass(param, **kwargs)
        except InterfaceOpenError as ex:
            logger.error(f"Failed to create interface client {ex}")
            meshtastic.util.our_exit(f"Error connecting to {ex.ifType} interface: {ex}", 1)
        except Exception as ex:
            logger.debug(f"Exception occurred: {ex}")
            meshtastic.util.our_exit(f"Exception during connection to radio occurred: {ex}", 1)
        return client