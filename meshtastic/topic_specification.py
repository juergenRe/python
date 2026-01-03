"""Specification of topics, their hierarchy and messages"""

class meshtastic:
    """parent topic, no direct messages defined """

    #---------------------------------------------------------------------------------
    # Internal topics, not for external use
    class mi_status_request:
        """defines the status request message (empty)"""

    class mi_status_publish:
        """defines the status message published"""

        def msgDataSpec(data: dict):
            """
            - data: dictionary of MeshStatus
            """
    class stcomm_start:
        """defines the trigger to start communication (empty)"""
        def msgDataSpec(timeout: int = 300):
            """starts the connection using the timeout value provided."""

    class stcomm_receive:
        """defines the message to data"""
        def msgDataSpec(field: str, data: dict):
            """
            - field: type of data received
            - data: dictionary of data received, corresponding to a protobuf field type
            """
    class stcomm_finish:
        """signals reaching the connected state (empty)"""
        def msgDataSpec(code: str):
            """indicates connected state and gives back error string.
            'OK' is success case
            """

    class channel_publish:
        """published new channel data"""
        def msgDataSpec(chanNo: int, data: dict):
            """
            - chanNo: channel number
            - channel data contents as dict
            """

    #---------------------------------------------------------------------------------
    # public topics for use with API
    class log:
        """used with subtopic"""
        class line:
            """defines a single line of log message"""
            def msgDataSpec(message: str, interface):
                """ line of "message" originating from "interface" """
