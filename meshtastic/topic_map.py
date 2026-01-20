"""List of all topics used within the project"""
import logging
from datetime import datetime
from typing import TextIO, List, Mapping

from pubsub import pub
from pubsub.core import Listener, Topic, Publisher, INotificationHandler
from pubsub.utils.notification import NotifyByWriteFile, IgnoreNotificationsMixin

# meshtastic topic specs
from meshtastic import topic_specification

from meshtastic import mt_config

# -- internal topic names --

# Start communication topics
SUBS_STARTCOMM_START = 'meshtastic.stcomm_start'        # Trigger receiving initial data
SUBS_STARTCOMM_RECEIVE = 'meshtastic.stcomm_receive'
SUBS_STARTCOMM_FINISH = 'meshtastic.stcomm_finish'

# Meshinterface Status
SUBS_MI_STATUS_REQ = 'meshtastic.mi_status_request'
SUBS_MI_STATUS_PUB = 'meshtastic.mi_status_publish'
SUBS_MI_CONNECTED = 'meshtastic.mi_connected'
SUBS_MI_DISCONNECT = 'meshtastic.mi_disconnect'

# Data publishing
SUBS_MY_INFO_PUB = 'meshtastic.my_info_publish'
SUBS_METADATA_PUB = 'meshtastic.metadata_publish'
SUBS_CHANNEL_PUB = 'meshtastic.channel_publish'
SUBS_CONFIG_PUB = 'meshtastic.config_publish'
#SUBS_MODULE_CONFIG_PUB = 'meshtastic.module_config_publish'

# Node info publising
SUBS_NODEINFO_PUB = 'meshtastic.nodeinfo_publish'

# Packet handling
SUBS_PACKET_REQ = 'meshtastic.packet.send_request'
SUBS_PACKET_PUB = 'meshtastic.packet.receive'

# Topic names used in API
TOPIC_LOG_LINE = 'meshtastic.log.line'
TOPIC_CONNECTED = 'meshtastic.connection.established'
TOPIC_DISCONNECTED = 'meshtastic.connection.lost'
TOPIC_RECEIVE_PACKET = 'meshtastic.receive.text'
TOPIC_RECEIVE_POSITION = 'meshtastic.receive.position'
TOPIC_RECEIVE_USER = 'meshtastic.receive.user'
TOPIC_RECEIVE_DATA = 'meshtastic.receive.data.portnum'
TOPIC_NODE_UPDATE = 'meshtastic.node.updated'
TOPIC_CLIENT_NOTIFY = 'meshtastic.clientNotification'

# import spec
logger = logging.getLogger(__name__)
logger.debug("Initialize topic map")
try:
    pub.addTopicDefnProvider(topic_specification, pub.TOPIC_TREE_FROM_CLASS)
    pub.setTopicUnspecifiedFatal()
except Exception as ex:
    logger.debug(f"Importing topic map failed: {ex}")


class NotifyByWriteFileEx(INotificationHandler):
    """
    Print a message to stdout or file when a notification is received.
    """
    defaultPrefix = 'PUBSUB:'

    def __init__(self, fileObj: TextIO = None, prefix: str = None, timeFormat: str = None):
        """
        Will write to stdout unless fileObj given. Will use
        defaultPrefix as prefix for each line output, unless prefix
        specified.
        """
        self.__pre = prefix or self.defaultPrefix
        if fileObj is None:
            import sys
            self.__fileObj = sys.stdout
        else:
            self.__fileObj = fileObj
        self.__timeFormat = timeFormat

    def changeFile(self, fileObj):
        self.__fileObj = fileObj

    def getTime(self) -> str:
        """Formats time to string"""
        if self.__timeFormat:
            ts =  datetime.now().strftime(self.__timeFormat)
        else:
            ts = datetime.now().isoformat()
        return ts

    def notifySubscribe(self, pubListener: Listener, topicObj: Topic, newSub: bool):
        flgRedun = ''
        if not newSub:
            flgRedun = 'redundant'
        msg = f"{self.getTime()} {self.__pre} Subscribe   L<{pubListener}> T<{topicObj.getName()}> {flgRedun}\n"
        self.__fileObj.write(msg)

    def notifyUnsubscribe(self, pubListener: Listener, topicObj: Topic):
        msg = f"{self.getTime()} {self.__pre} Unsubscribe L<{pubListener}> T<{topicObj.getName()}>\n"
        self.__fileObj.write(msg)

    def notifyDeadListener(self, pubListener: Listener, topicObj: Topic):
        msg = f"{self.getTime()} {self.__pre} Dead Listn  L<{pubListener}> T<{topicObj.getName()}>\n"
        # a bug apparently: sometimes on exit, the stream gets closed before
        # and leads to a TypeError involving NoneType
        self.__fileObj.write(msg)

    def notifySend(self, stage: str, topicObj: Topic, pubListener: Listener = None):
        if stage == 'in':
            stage = 'In    '
        elif stage == 'pre':
            stage = 'Pre   '
        else:
            stage = 'Post  '
        msg = f"{self.getTime()} {self.__pre} Send {stage} L<{pubListener}> T<{topicObj.getName()}>\n"
        self.__fileObj.write(msg)

    def notifyNewTopic(self, topicObj: Topic, description: str, required: List[str], argsDocs: Mapping[str, str]):
        msg = f"{self.getTime()} {self.__pre} Create T<{topicObj.getName()}>\n"
        self.__fileObj.write(msg)

    def notifyDelTopic(self, topicName: str):
        msg = f"{self.getTime()} {self.__pre} Delete T<{topicName}>\n"
        self.__fileObj.write(msg)


def useNotifyByWriteFileEx(fileObj: TextIO = None, prefix: str = None, publisher: Publisher = None, all: bool = True,
                         **kwargs):
    """
    Will cause all pubsub notifications of pubsub "actions" (such as new topic created, message sent, listener died
    etc) to be written to specified file (or stdout if none given). The fileObj need only provide a 'write(string)'
    method.

    The first two arguments are the same as those of NotifyByWriteFile constructor. The 'all' and kwargs arguments
    are those of pubsub's setNotificationFlags(), except that 'all' defaults to True.  See useNotifyByPubsubMessage()
    for an explanation of pubModule (typically only if pubsub inside wxPython's wx.lib)
    """

    if publisher is None:
        # from .. import pub
        publisher = pub.getDefaultPublisher()
    notifHandler = NotifyByWriteFileEx(fileObj, prefix)

    publisher.addNotificationHandler(notifHandler)
    publisher.setNotificationFlags(all=all, **kwargs)