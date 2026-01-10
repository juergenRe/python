import abc
import enum
from typing import Any, Callable

from google.protobuf.message import Message
from meshtastic.mesh_model import MeshModel

class CmdError(enum.Enum):
    """Enumerations for command return/error values"""
    OK = 0
    ERROR = 1   # generic error without further spec
    TIMEOUT = 2
    NO_DATA = 3
    INCOMPLETE_DATA = 4


class ICommand(metaclass=abc.ABCMeta):
    """The interface definition for all protocol handlers"""
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'execute') and
                callable(subclass.execute) or
                NotImplemented)

    @abc.abstractmethod
    def execute(self, model: MeshModel, timeout: int) -> tuple[CmdError, str]:
        """Executes the command"""
        raise NotImplementedError(f"Command not implemented")

