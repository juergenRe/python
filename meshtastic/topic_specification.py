"""Specification of topics, their hierarchy and messages"""

class meshtastic:
    """parent topic, no direct messages defined """

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