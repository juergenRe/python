"""List of all topics used within the project"""
import logging
import sys

from pubsub import pub
from pubsub.utils.notification import useNotifyByWriteFile, IgnoreNotificationsMixin

# meshtastic topic specs
import topic_specification

# -- internal topic names --

# Start communication topics
SUBS_STARTCOMM_START = 'meshtastic.stcomm_start'        # Trigger receiving initial data
SUBS_STARTCOMM_RECEIVE = 'meshtastic.stcomm_receive'
SUBS_STARTCOMM_FINISH = 'meshtastic.stcomm_finish'

# Meshinterface Status
SUBS_MI_STATUS_REQ = 'meshtastic.mi_status_request'
SUBS_MI_STATUS_PUB = 'meshtastic.mi_status_publish'


# import spec
logger = logging.getLogger(__name__)
logger.debug("Initialize topic map")
try:
    pub.addTopicDefnProvider(topic_specification, pub.TOPIC_TREE_FROM_CLASS)
    pub.setTopicUnspecifiedFatal()
except Exception as ex:
    logger.debug(f"Importing topic map failed: {ex}")



# create one special notification handler that ignores all except
# one type of notification
class MyPubsubNotifHandler(IgnoreNotificationsMixin):
    def notifySubscribe(self, pubListener, topicObj, newSub):
        newSubMsg = ''
        if not newSub:
            newSubMsg = ' was already'
        msg = 'MyPubsubNotifHandler: listener %s%s subscribed to %s'
        print(msg % (pubListener.name(), newSubMsg, topicObj.getName()))


pub.addNotificationHandler(MyPubsubNotifHandler())

# print(all notifications to stdout)
useNotifyByWriteFile(sys.stdout, prefix='NotifyByWriteFile:')