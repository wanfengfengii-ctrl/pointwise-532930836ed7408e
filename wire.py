"""The only replica-to-reconciler transport; all responses are counted."""
import json


def encode(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode(data):
    return json.loads(data.decode("utf-8"))


class Client:
    def __init__(self, service):
        self.__service = service
        self.response_bytes = 0

    def reset_meter(self):
        self.response_bytes = 0

    def request(self, snapshot, operation, **arguments):
        raw = self.__service.dispatch(encode({
            "snapshot": snapshot, "operation": operation, "arguments": arguments
        }))
        if not isinstance(raw, bytes):
            raise TypeError("dispatch must return bytes")
        self.response_bytes += len(raw)
        answer = decode(raw)
        if answer["snapshot"] != snapshot:
            raise ValueError("snapshot mismatch")
        return answer["data"]
