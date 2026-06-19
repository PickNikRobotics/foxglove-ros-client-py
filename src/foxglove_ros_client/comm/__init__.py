class RosBridgeException(Exception):
    pass


class RosBridgeProtocol:
    pass


class RosBridgeClientFactory:
    @staticmethod
    def create_url(host, port=None, is_secure=False):
        if host.startswith(("ws://", "wss://", "http://", "https://")):
            return host
        scheme = "wss" if is_secure else "ws"
        if port is None:
            raise ValueError("Port must be set when host is not a websocket URL")
        return f"{scheme}://{host}:{port}"


__all__ = ["RosBridgeException", "RosBridgeProtocol", "RosBridgeClientFactory"]
