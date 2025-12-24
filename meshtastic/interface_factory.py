"""Factory to create the specific connection to the radio according to the arguments passed"""

from typing import Union

import logging

from meshtastic.radio_interface import InterfaceOpenError, IRadioInterface
from meshtastic.serial_interface import SerialInterface
from meshtastic.tcp_interface import TCPInterface, DEFAULT_TCP_PORT
from meshtastic.ble_interface import BLEInterface
from meshtastic.util import findPorts, our_exit
#import meshtastic.util

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

    def findSerialPort(self) -> str | None:
        """Try to find a serial port for a radio to connect to.
        If there are more than one serial pport available: abort
        return the name of the port, None otherwise"""
        ports: list[str] = findPorts(True)
        logger.debug(f"ports:{ports}")
        if len(ports) == 0:
            print("No Serial Meshtastic device detected, attempting TCP connection on localhost.")
            return None
        elif len(ports) > 1:
            message: str = "Warning: Multiple serial ports were detected so one serial port must be specified with the '--port'.\n"
            message += f"  Ports detected:{ports}"
            logger.error(message)
            our_exit(message)
        else:
            return ports[0]

    def _createInterfaceInt(self,
                         ble: str = None,
                         host: str = None,
                         serial: str = None,
                         level: int = 0,
                         **kwargs) -> IRadioInterface| None:
        """internal function to create the interface with fallback in case of error on opening"""
        ifceClass, param = self.selectInterfaceType(ble, host, serial)
        if ifceClass is None:
            # no interface defined. Try to find a serial port to use
            serial = self.findSerialPort()
            ifceClass, param = self.selectInterfaceType(ble, host, serial)

            # if still no port available, select tcp and localhost (this might fail when trying to connect)
            if ifceClass is None:
                ifceClass, param = self.selectInterfaceType(ble, f"localhost:{DEFAULT_TCP_PORT}", None)

        try:
            # connect to interface with standard signature:
            # address, connectNow, timeout, noNodes, debugOut, noProto
            client = ifceClass(param, **kwargs)
        except InterfaceOpenError as ex:
            # check to retry on 'localhost' when connection to serial fails
            if level == 0 and ex.ifType == 'serial':
                return self._createInterfaceInt(ble, f"localhost:{DEFAULT_TCP_PORT}", None, 1, **kwargs)
            logger.error(f"Failed to create interface client {ex}")
            our_exit(f"Error connecting to {ex.ifType} interface: {ex}", 1)
        except Exception as ex:
            logger.debug(f"Exception occurred: {ex}")
            our_exit(f"Exception during connection to radio occurred: {ex}", 1)
        return client

    def createInterface(self,
                    ble: str = None,
                    host: str = None,
                    serial: str = None,
                    **kwargs) -> IRadioInterface | None:
        """create an instance of the needed interface according passed kwargs"""
        if host:
            if ":" not in host:
                host = f"{host}:{DEFAULT_TCP_PORT}"

        return self._createInterfaceInt(ble, host, serial, 0, **kwargs)
