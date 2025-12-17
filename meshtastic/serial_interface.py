""" Serial interface class
"""
# pylint: disable=R0917
import logging
import os
import sys
import time
from io import TextIOWrapper

from typing import List, Optional

import serial # type: ignore[import-untyped]

from meshtastic.mesh_interface import InterfaceOpenError
from meshtastic.util import is_windows11, findPorts, our_exit, isSiLabPort
from meshtastic.stream_interface import StreamInterface

logger = logging.getLogger(__name__)

class SerialInterface(StreamInterface):
    """Interface class for meshtastic devices over a serial link"""

    def __init__(
        self,
        devPath: Optional[str] = None,
        connectNow: bool = True,
        timeout: int = 300,
        noNodes: bool = False,
        noProto: bool = False,
        debugOut: TextIOWrapper | None = None
    ) -> None:
        """Constructor, opens a connection to a specified serial port, or if unspecified try to
        find one Meshtastic device by probing

        Keyword Arguments:
            devPath {string} -- A filepath to a device, i.e. /dev/ttyUSB0 (default: {None})
            debugOut {stream} -- If a stream is provided, any debug serial output from the device will be emitted to that stream. (default: {None})
            timeout -- How long to wait for replies (default: 300 seconds)
        """
        super().__init__(timeout, noNodes, debugOut, noProto)
        self.stream: serial.Serial
        self.devPath: Optional[str] = devPath
        self.is_windows11 = is_windows11()

        if self.devPath is None:
            ports: List[str] = findPorts(True)
            logger.debug(f"ports:{ports}")
            if len(ports) == 0:
                print("No Serial Meshtastic device detected, attempting TCP connection on localhost.")
                return
            elif len(ports) > 1:
                message: str = "Warning: Multiple serial ports were detected so one serial port must be specified with the '--port'.\n"
                message += f"  Ports detected:{ports}"
                our_exit(message)
            else:
                self.devPath = ports[0]

        logger.debug(f"Connecting to {self.devPath}")
        self.stream = self.open(self.devPath)
        if self.stream and connectNow:
            self.connectAndGetConfig()

    def open(self, comPath: str) -> serial.Serial:
        """Opens the serial line and provides the connection as stream"""
        isSiLab = isSiLabPort(self.devPath)
        logger.debug(f'Opening serial port: Platform: {sys.platform} port: {self.devPath} Driver: {isSiLab}')

        try:
            if sys.platform != "win32":
                stream = self.serLinuxOpen(comPath)
            elif isSiLab:
                stream = self.serWinOpenSiLabs(comPath)
            else:
                stream = self.serWinOpenNorm(comPath)

            stream.flush()	# type: ignore[attr-defined]
            time.sleep(0.1)
            return stream

        except serial.SerialException as ex:
            message = f"Serial Exception:\n"
            message += f"  The serial device at '{self.devPath}' cannot be opened or configured.\n"
            message += "  Please check the following:\n"
            message += "    1. Is the device connected properly?\n"
            message += "    2. Is the correct serial port specified?\n"
            message += "    3. Are the necessary drivers installed?\n"
            message += "    4. Are you using a **power-only USB cable**? A power-only cable cannot transmit data.\n"
            message += "       Ensure you are using a **data-capable USB cable**.\n"
            message += f"Detail message: {ex.args[0]}"
            raise InterfaceOpenError(message, 'serial')
        except FileNotFoundError:
            # Handle the case where the serial device is not found
            message = f"File Not Found Error:\n"
            message += f"  The serial device at '{self.devPath}' was not found.\n"
            message += "  Please check the following:\n"
            message += "    1. Is the device connected properly?\n"
            message += "    2. Is the correct serial port specified?\n"
            message += "    3. Are the necessary drivers installed?\n"
            message += "    4. Are you using a **power-only USB cable**? A power-only cable cannot transmit data.\n"
            message += "       Ensure you are using a **data-capable USB cable**.\n"
            raise InterfaceOpenError(message, 'serial')
        except PermissionError as ex:
            username = os.getlogin()
            message = "Permission Error:\n"
            message += "  Need to add yourself to the 'dialout' group by running:\n"
            message += f"     sudo usermod -a -G dialout {username}\n"
            message += "  After running that command, log out and re-login for it to take effect.\n"
            raise InterfaceOpenError(message, 'serial')
        except OSError as ex:
            message = f"OS Error:\n"
            message += "  The serial device couldn't be opened, it might be in use by another process.\n"
            message += "  Please close any applications or webpages that may be using the device and try again.\n"
            message += f"\nOriginal error: {ex}"
            raise InterfaceOpenError(message, 'serial')

    def serLinuxOpen(self, comPath: str) -> serial.Serial:
        """Opens serial port on Linux"""
        with open(comPath, encoding="utf8") as f:
            self._set_hupcl_with_termios(f)
        time.sleep(0.1)
        stream = serial.Serial(comPath, 115200, exclusive=True, timeout=0.5, write_timeout=0)
        return stream

    def serWinOpenNorm(self, comPath: str) -> serial.Serial:
        """Opens serial port on Windows"""
        stream = serial.Serial(comPath, 115200, exclusive=True, timeout=0.5, write_timeout=0)
        return stream

    def serWinOpenSiLabs(self, comPath: str) -> serial.Serial:
        """Opens serial port on Windows with a SiLab driver"""
        stream = serial.Serial(None, 115200, exclusive=True, timeout=0.5, write_timeout=0)
        stream.port = comPath
        stream.dtr = 0
        stream.rts = 0
        stream.open()
        return stream

    def _set_hupcl_with_termios(self, f: TextIOWrapper):
        """first we need to set the HUPCL so the device will not reboot based on RTS and/or DTR
        see https://github.com/pyserial/pyserial/issues/124
        """
        if sys.platform == "win32":
            return

        import termios  # pylint: disable=C0415,E0401
        attrs = termios.tcgetattr(f)
        attrs[2] = attrs[2] & ~termios.HUPCL
        termios.tcsetattr(f, termios.TCSAFLUSH, attrs)

    def __repr__(self):
        rep = f"SerialInterface(devPath={self.devPath!r}"
        if hasattr(self, 'debugOut') and self.debugOut is not None:
            rep += f", debugOut={self.debugOut!r}"
        if self.noProto:
            rep += ", noProto=True"
        if hasattr(self, 'noNodes') and self.noNodes:
            rep += ", noNodes=True"
        rep += ")"
        return rep

    def _writeBytes(self, b: bytes) -> None:
        """Write an array of bytes to our stream and flush"""
        if self.stream:  # ignore writes when stream is closed
            self.stream.write(b)
            self.stream.flush()
            # win11 might need a bit more time, too
            if self.is_windows11:
                time.sleep(1.0)
            else:
                # we sleep here to give the TBeam a chance to work
                time.sleep(0.1)

    def _readBytes(self, length) -> Optional[bytes]:
        """Read an array of bytes from our stream"""
        if self.stream:
            return self.stream.read(length)
        else:
            return None

    def close(self) -> None:
        """Close a connection to the device"""
        if self.stream:  # Stream can be null if we were already closed
            self.stream.flush()  # FIXME: why are there these  two flushes with 100ms sleeps?  This shouldn't be necessary
            time.sleep(0.1)
            self.stream.flush()
            time.sleep(0.1)
        logger.debug("Closing Serial stream")
        StreamInterface.close(self)
