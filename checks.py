"""Public correctness oracle and business demo; no timing in this suite."""
import json
import random
import sys
from frozen import FrozenReplica, linear_diff
from wire import Client
from samples import ROWS, LEFT_CHANGES, RIGHT_CHANGES, EXPECTED


def oracle(left, right):
    return [(k, left.get(k), right.get(k))
            for k in sorted(left.keys() | right.keys(), key=lambda x: x.encode("utf-8"))
            if left.get(k) != right.get(k)]


def mutate(rows, changes):
    for key, value in changes.items():
        if value is None:
            rows.pop(key, None)
        else:
            rows[key] = value


def implementation(mode):
    if mode == "reference":
        return FrozenReplica, linear_diff
    from replica import Replica
    from reconcile import diff
    return Replica, diff


def suite(mode):
    Replica, diff = implementation(mode)
    l, r = Replica(ROWS), Replica(dict(reversed(list(ROWS.items()))))
    lc, rc = Client(l), Client(r)
    old_l, old_r = l.snapshot(), r.snapshot()
    ls, rs = l.apply(LEFT_CHANGES), r.apply(RIGHT_CHANGES)
    assert diff(lc, ls, rc, rs) == EXPECTED
    assert diff(lc, old_l, rc, old_r) == []
    # Independently materialize each expected snapshot, including cross-generation comparisons.
    lm, rm = dict(ROWS), dict(ROWS)
    mutate(lm, LEFT_CHANGES)
    mutate(rm, RIGHT_CHANGES)
    assert diff(lc, ls, rc, old_r) == oracle(lm, ROWS)
    assert diff(lc, old_l, rc, rs) == oracle(ROWS, rm)
    # The original scan remains usable, with exact half-open domains and pagination.
    page = lc.request(ls, "scan", lo="a", hi="b", after=None, limit=1)
    assert page == {"rows": [["a", b"new".hex()]], "more": True}
    page2 = lc.request(ls, "scan", lo="a", hi="b", after="a", limit=1)
    assert page2 == {"rows": [["aa", b"same".hex()]], "more": False}
    assert lc.request(ls, "scan", lo="a", hi="a", after=None, limit=2) == {"rows": [], "more": False}
    try:
        lc.request(old_r, "scan", lo=None, hi=None, after=None, limit=1)
    except ValueError:
        pass
    else:
        raise AssertionError("foreign snapshot accepted")
    # Input ownership and empty batches also obey immutable snapshot semantics.
    source = {"owned": b"before"}
    owned = Replica(source)
    source["owned"] = b"outside"
    changes = {"owned": b"inside"}
    published = owned.apply(changes)
    changes["owned"] = b"outside-again"
    no_op = owned.apply({})
    assert no_op != published
    empty_service = Replica({})
    assert diff(Client(owned), published, Client(empty_service), empty_service.snapshot()) == [
        ("owned", b"inside", None)]
    assert diff(Client(owned), published, Client(owned), no_op) == []
    # Empty maps, deleting the final key, reinserting a boundary and retained snapshots.
    e, one = Replica({}), Replica({"a": b""})
    ec, oc = Client(e), Client(one)
    before, empty = one.snapshot(), one.apply({"a": None})
    assert diff(ec, e.snapshot(), oc, empty) == []
    assert diff(ec, e.snapshot(), oc, before) == [("a", None, b"")]
    newer = one.apply({"": b"zero", "a": b"", "aa": b"x", "a\x00": b"n"})
    expected = {"": b"zero", "a": b"", "aa": b"x", "a\x00": b"n"}
    assert diff(ec, e.snapshot(), oc, newer) == oracle({}, expected)
    assert diff(oc, empty, oc, newer) == oracle({}, expected)
    assert diff(oc, newer, oc, newer) == []
    # Deterministic mixed edits; keep every earlier snapshot alive and revisit it.
    rng = random.Random(81723)
    keys = ["", "\x00", "a", "aa", "a\x00", "a\x00x", "é", "水", "😀"]
    keys += ["k%03d" % i for i in range(180)]
    maps = [{}, {}]
    services = [Replica({}), Replica({})]
    clients = [Client(x) for x in services]
    history = [[(services[i].snapshot(), {})] for i in range(2)]
    for batch in range(45):
        side = batch % 2
        changes = {rng.choice(keys): rng.choice([None, b"", b"x", bytes([batch]), b"\x00\xff"])
                   for _ in range(9)}
        token = services[side].apply(changes)
        mutate(maps[side], changes)
        history[side].append((token, dict(maps[side])))
        a, b = history[0][-1], history[1][-1]
        assert diff(clients[0], a[0], clients[1], b[0]) == oracle(a[1], b[1])
        a, b = rng.choice(history[0]), rng.choice(history[1])
        assert diff(clients[0], a[0], clients[1], b[0]) == oracle(a[1], b[1])
    return {"suite": mode, "correct": True}


def demo():
    Replica, diff = implementation("baseline")
    l, r = Replica(ROWS), Replica(ROWS)
    result = diff(Client(l), l.apply(LEFT_CHANGES), Client(r), r.apply(RIGHT_CHANGES))
    assert result == EXPECTED
    return {"encoding": "hex", "differences": [
        [k, None if a is None else a.hex(), None if b is None else b.hex()]
        for k, a, b in result]}


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "demo":
        print(json.dumps(demo(), ensure_ascii=False, sort_keys=True))
    elif mode in ("baseline", "target"):
        result = suite(mode)
        if mode == "target":
            import bench
            result["benchmark"] = bench.run("target")
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        raise ValueError("expected baseline, target or demo")
