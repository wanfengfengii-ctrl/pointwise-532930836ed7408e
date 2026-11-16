"""Frozen, correct full-copy snapshots and paginated linear comparison."""
from bisect import bisect_left, bisect_right
from uuid import uuid4
from wire import encode, decode


class FrozenReplica:
    def __init__(self, rows):
        self._prefix = uuid4().hex
        self._versions = {}
        self._serial = -1
        self._publish(dict(rows))

    def _publish(self, mapping):
        self._serial += 1
        token = self._prefix + ":" + str(self._serial)
        keys = tuple(sorted(mapping, key=lambda k: k.encode("utf-8")))
        self._versions[token] = (mapping, keys, tuple(k.encode("utf-8") for k in keys))
        self._head = token
        return token

    def snapshot(self):
        return self._head

    def apply(self, changes):
        mapping = self._versions[self._head][0].copy()
        for key, value in changes.items():
            if value is None:
                mapping.pop(key, None)
            else:
                mapping[key] = value
        return self._publish(mapping)

    def dispatch(self, request):
        req = decode(request)
        token = req["snapshot"]
        if token not in self._versions:
            raise ValueError("unknown snapshot")
        if req["operation"] != "scan":
            raise ValueError("unknown operation")
        args = req["arguments"]
        lo, hi, after, limit = (args[x] for x in ("lo", "hi", "after", "limit"))
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ValueError("invalid limit")
        if lo is not None and hi is not None and lo.encode("utf-8") > hi.encode("utf-8"):
            raise ValueError("invalid range")
        mapping, keys, encoded = self._versions[token]
        start = 0 if lo is None else bisect_left(encoded, lo.encode("utf-8"))
        stop = len(keys) if hi is None else bisect_left(encoded, hi.encode("utf-8"))
        if after is not None:
            start = max(start, bisect_right(encoded, after.encode("utf-8")))
        end = min(stop, start + limit)
        selected = keys[start:end] if start < stop else ()
        data = {"rows": [[k, mapping[k].hex()] for k in selected],
                "more": end < stop}
        return encode({"snapshot": token, "data": data})


def _records(client, snapshot):
    after = None
    while True:
        page = client.request(snapshot, "scan", lo=None, hi=None,
                              after=after, limit=1024)
        rows = page["rows"]
        for key, value in rows:
            yield key, bytes.fromhex(value)
        if not page["more"]:
            return
        if not rows:
            raise AssertionError("empty nonterminal page")
        after = rows[-1][0]


def linear_diff(left, left_snapshot, right, right_snapshot):
    lhs, rhs = iter(_records(left, left_snapshot)), iter(_records(right, right_snapshot))
    a, b = next(lhs, None), next(rhs, None)
    out = []
    while a is not None or b is not None:
        if b is None or (a is not None and a[0].encode("utf-8") < b[0].encode("utf-8")):
            out.append((a[0], a[1], None))
            a = next(lhs, None)
        elif a is None or b[0].encode("utf-8") < a[0].encode("utf-8"):
            out.append((b[0], None, b[1]))
            b = next(rhs, None)
        else:
            if a[1] != b[1]:
                out.append((a[0], a[1], b[1]))
            a, b = next(lhs, None), next(rhs, None)
    return out
