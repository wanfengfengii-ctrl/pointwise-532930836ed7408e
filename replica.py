"""Optimized replica service: immutable treap snapshots with incremental,
order-independent range summaries.

Design notes:
- Each snapshot is the root of a persistent treap ordered by UTF-8 key
  bytes, with deterministic priorities derived from the record digest.
  apply() path-copies only the O(log n) spine per changed key, so old
  snapshots stay readable forever without copying or rebuilding the index.
  The initial build is a one-pass O(n) Cartesian tree construction.
- Every node caches its subtree record count and subtree content digest:
  the sum (mod 2**256) of per-record SHA256 digests. The digest of any
  half-open key domain is assembled from O(log n) cached nodes and depends
  only on the current records inside the domain, never on the tree shape
  or the correction history, so equal content compares equal even when the
  two sides partitioned their indexes differently.
- All reads go through dispatch() and wire.encode, so the metered Client
  remains the only transport; there is no uncounted side channel.
"""
import hashlib
from uuid import uuid4

from wire import decode, encode

_MOD = 1 << 256


def _record_digest(enc, value):
    """Unambiguous per-record digest: length-prefixed key and value.

    Distinct keys, key prefixes, empty values and missing keys can never be
    conflated: the key and value are each length-prefixed before hashing,
    and a missing key simply contributes no record digest at all.
    """
    h = hashlib.sha256()
    h.update(len(enc).to_bytes(8, "big"))
    h.update(enc)
    h.update(len(value).to_bytes(8, "big"))
    h.update(value)
    return h.digest()


class _Node:
    __slots__ = ("enc", "key", "value", "prio", "rec", "left", "right",
                 "count", "total", "mn", "mx")

    def __init__(self, enc, key, value, digest):
        self.enc = enc                # UTF-8 bytes of the key (ordering)
        self.key = key                # original string key
        self.value = value            # bytes value
        self.rec = int.from_bytes(digest, "big")
        self.prio = int.from_bytes(digest[:8], "big")
        self.left = None
        self.right = None
        self.count = 1                # records in subtree
        self.total = self.rec         # sum of subtree record digests mod 2**256
        self.mn = enc                 # min key bytes in subtree
        self.mx = enc                 # max key bytes in subtree


def _recompute(node):
    left = node.left
    right = node.right
    count = 1
    total = node.rec
    mn = node.enc
    mx = node.enc
    if left is not None:
        count += left.count
        total += left.total
        mn = left.mn
    if right is not None:
        count += right.count
        total += right.total
        mx = right.mx
    node.count = count
    node.total = total % _MOD
    node.mn = mn
    node.mx = mx


def _clone(node, left, right):
    """New node copying an existing record with replaced children."""
    out = _Node.__new__(_Node)
    out.enc = node.enc
    out.key = node.key
    out.value = node.value
    out.prio = node.prio
    out.rec = node.rec
    out.left = left
    out.right = right
    _recompute(out)
    return out


def _split(node, bound):
    """Persistent split into (keys < bound, keys >= bound)."""
    if node is None:
        return (None, None)
    if node.enc < bound:
        low, high = _split(node.right, bound)
        return (_clone(node, node.left, low), high)
    low, high = _split(node.left, bound)
    return (low, _clone(node, high, node.right))


def _merge(a, b):
    """Persistent merge; every key of a strictly precedes every key of b."""
    if a is None:
        return b
    if b is None:
        return a
    if a.prio >= b.prio:
        return _clone(a, a.left, _merge(a.right, b))
    return _clone(b, _merge(a, b.left), b.right)


def _put(root, enc, key, value):
    low, rest = _split(root, enc)
    _, rest = _split(rest, enc + b"\x00")  # isolate any existing record
    leaf = _Node(enc, key, value, _record_digest(enc, value))
    return _merge(low, _merge(leaf, rest))


def _delete(root, enc):
    low, rest = _split(root, enc)
    _, rest = _split(rest, enc + b"\x00")  # drop the isolated record, if any
    return _merge(low, rest)


def _build(items):
    """O(n) Cartesian treap build from sorted (enc, key, value, digest)."""
    stack = []
    for enc, key, value, digest in items:
        node = _Node(enc, key, value, digest)
        last = None
        while stack and stack[-1].prio < node.prio:
            last = stack.pop()
        node.left = last
        if stack:
            stack[-1].right = node
        stack.append(node)
    if not stack:
        return None
    order = []
    todo = [stack[0]]
    while todo:
        node = todo.pop()
        order.append(node)
        if node.left is not None:
            todo.append(node.left)
        if node.right is not None:
            todo.append(node.right)
    for node in reversed(order):
        _recompute(node)
    return stack[0]


def _range_summary(root, lo, hi):
    """(count, digest) of the records inside the half-open domain [lo, hi).

    Assembled from O(log n) cached subtree aggregates; the result depends
    only on the domain content, not on the internal node layout.
    """
    count = 0
    total = 0
    stack = [root]
    while stack:
        node = stack.pop()
        if node is None:
            continue
        if lo is not None and node.mx < lo:
            continue
        if hi is not None and node.mn >= hi:
            continue
        if (lo is None or node.mn >= lo) and (hi is None or node.mx < hi):
            count += node.count
            total += node.total
            continue
        if (lo is None or node.enc >= lo) and (hi is None or node.enc < hi):
            count += 1
            total += node.rec
        stack.append(node.left)
        stack.append(node.right)
    return count, total % _MOD


def _rank(root, bound):
    """Number of records with key bytes < bound."""
    rank = 0
    node = root
    while node is not None:
        if bound <= node.enc:
            node = node.left
        else:
            rank += 1
            if node.left is not None:
                rank += node.left.count
            node = node.right
    return rank


def _select(root, index):
    """Node at global 0-based rank index, or None."""
    node = root
    while node is not None:
        left = node.left
        left_count = left.count if left is not None else 0
        if index < left_count:
            node = left
        elif index == left_count:
            return node
        else:
            index -= left_count + 1
            node = node.right
    return None


def _scan(root, lo, hi, limit):
    """Up to limit rows of [lo, hi) in key order, plus the more flag."""
    rows = []
    stack = []
    node = root
    while node is not None:
        if lo is not None and node.enc < lo:
            node = node.right
        else:
            stack.append(node)
            node = node.left
    more = False
    while stack:
        node = stack.pop()
        if hi is not None and node.enc >= hi:
            break
        if len(rows) == limit:
            more = True
            break
        rows.append([node.key, node.value.hex()])
        node = node.right
        while node is not None:
            stack.append(node)
            node = node.left
    return rows, more


class Replica:
    """Optimized replica; interface-compatible with frozen.FrozenReplica."""

    def __init__(self, rows):
        self._prefix = uuid4().hex
        self._versions = {}
        self._serial = -1
        # Take over an independent copy: keys/values are immutable, so
        # extracting them into nodes isolates us from later caller edits.
        items = [(key.encode("utf-8"), key, value)
                 for key, value in rows.items()]
        items.sort()
        self._publish(_build([(enc, key, value, _record_digest(enc, value))
                              for enc, key, value in items]))

    def _publish(self, root):
        self._serial += 1
        token = "%s:%d" % (self._prefix, self._serial)
        self._versions[token] = root
        self._head = token
        return token

    def snapshot(self):
        return self._head

    def apply(self, changes):
        root = self._versions[self._head]
        for key, value in changes.items():
            enc = key.encode("utf-8")
            if value is None:
                root = _delete(root, enc)
            else:
                root = _put(root, enc, key, value)
        # Even an empty or no-op batch publishes a fresh snapshot token.
        return self._publish(root)

    def dispatch(self, request):
        req = decode(request)
        token = req["snapshot"]
        if token not in self._versions:
            raise ValueError("unknown snapshot")
        root = self._versions[token]
        op = req["operation"]
        args = req["arguments"]
        if op == "scan":
            data = self._scan_data(root, args)
        elif op == "summaries":
            data = self._summaries_data(root, args)
        elif op == "fetch":
            data = self._fetch_data(root, args)
        else:
            raise ValueError("unknown operation")
        return encode({"snapshot": token, "data": data})

    @staticmethod
    def _scan_data(root, args):
        lo = args["lo"]
        hi = args["hi"]
        after = args["after"]
        limit = args["limit"]
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ValueError("invalid limit")
        lo_b = lo.encode("utf-8") if lo is not None else None
        hi_b = hi.encode("utf-8") if hi is not None else None
        if lo_b is not None and hi_b is not None and lo_b > hi_b:
            raise ValueError("invalid range")
        if after is not None:
            # "strictly greater than after" as a byte-level lower bound.
            bound = after.encode("utf-8") + b"\x00"
            if lo_b is None or lo_b < bound:
                lo_b = bound
        rows, more = _scan(root, lo_b, hi_b, limit)
        return {"rows": rows, "more": more}

    @staticmethod
    def _summaries_data(root, args):
        """Per-domain (count, digest, median key) for the requested snapshot.

        The digest commits to the exact record set of the domain on this
        snapshot; mid is a split hint at the domain's median key.
        """
        results = []
        for lo, hi in args["domains"]:
            lo_b = lo.encode("utf-8") if lo is not None else None
            hi_b = hi.encode("utf-8") if hi is not None else None
            count, total = _range_summary(root, lo_b, hi_b)
            mid = None
            if count:
                base = _rank(root, lo_b) if lo_b is not None else 0
                mid = _select(root, base + count // 2).key
            results.append({"count": count,
                            "digest": "%064x" % total,
                            "mid": mid})
        return {"results": results}

    @staticmethod
    def _fetch_data(root, args):
        """Full record lists of the requested small domains."""
        results = []
        for lo, hi in args["domains"]:
            lo_b = lo.encode("utf-8") if lo is not None else None
            hi_b = hi.encode("utf-8") if hi is not None else None
            rows, _ = _scan(root, lo_b, hi_b, 1 << 30)
            results.append(rows)
        return {"results": results}
