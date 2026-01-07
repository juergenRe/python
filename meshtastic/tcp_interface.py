"""TCPInterface class for interfacing with http endpoint
"""
# pylint: disable=R0917
import contextlib
import logging
import socket
import time
from typing import Optional, Callable
from io import TextIOWrapper

from meshtastic.stream_interface import StreamInterface

DEFAULT_TCP_PORT = 4403
logger = logging.getLogger(__name__)

class TCPInterface(StreamInterface):
    """Interface class for meshtastic devices over a TCP link"""

    def __init__(
        self,
        address: str,
        rcvCallback: Callable[[bytes], None],
        logCallback: Callable[[str], None],
    ):
        """Constructor, opens a connection to a specified IP address/hostname

        Keyword Arguments:
            hostname {string} -- Hostname/IP address of the device to connect to
            timeout -- How long to wait for replies (default: 300 seconds)
        """

        super().__init__(address, rcvCallback, logCallback)

        hn = address.split(':', maxsplit=1)
        self.serverAddress = (hn[0], int(hn[1]))
        self.socket: socket.socket | None = None

        self.open()

    def __repr__(self):
        return f"TCPInterface(address:{self.serverAddress!r}"

    def _socket_shutdown(self) -> None:
        """Shutdown the socket.
        Note: Broke out this line so the exception could be unit tested.
        """
        if self.socket is not None:
            self.socket.shutdown(socket.SHUT_RDWR)

    def open(self) -> None:
        """Connect to socket"""
        logger.debug(f"Connecting to {self.serverAddress}") # type: ignore[str-bytes-safe]
        self.socket = socket.create_connection(self.serverAddress)

    def close(self) -> None:
        """Close a connection to the device"""
        logger.debug("Closing TCP stream")
        super().close()
        # Sometimes the socket read might be blocked in the reader thread.
        # Therefore, we force the shutdown by closing the socket here
        if self.socket is not None:
            with contextlib.suppress(Exception):  # Ignore errors in shutdown, because we might have a race with the server
                self._socket_shutdown()
            self.socket.close()
        self.socket = None

    def _writeBytes(self, b: bytes) -> None:
        """Write an array of bytes to our stream and flush"""
        if self.socket is not None:
            self.socket.send(b)

    def _readBytes(self, length) -> Optional[bytes]:
        """Read an array of bytes from our stream"""
        if self.socket is not None:
            data = self.socket.recv(length)
            # empty byte indicates a disconnected socket,
            # we need to handle it to avoid an infinite loop reading from null socket
            if data == b'':
                logger.debug("dead socket, re-connecting")
                # cleanup and reconnect socket without breaking reader thread
                with contextlib.suppress(Exception):
                    self._socket_shutdown()
                self.socket.close()
                self.socket = None
                time.sleep(1)
                self.open()
                self._startConfig()
                return None
            return data

        # no socket, break reader thread
        self._wantExit = True
        return None
