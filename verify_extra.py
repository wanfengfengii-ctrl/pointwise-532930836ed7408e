"""Additional differential and boundary verification for the optimized
replica service and reconciler.

Complements the frozen suite (checks.py). Every expected result is computed
independently, either with the dict oracle or with the frozen full-copy
reference implementation; the optimized internals are never consulted for
expectations. Deterministic seeds; safe to re-run.

Run: python3 verify_extra.py
"""
import random

from checks import mutate, oracle
from frozen import FrozenReplica, linear_diff
from reconcile import diff
from replica import Replica
from wire import Client

KEY_POOL = ["", "\x00", "a", "aa", "aaa", "a\x00", "a\x00x", "ab", "b",
            "é", "水", "😀", "z", "zz", "\xff"]
KEY_POOL += ["k%03d" % i for i in range(120)]
VALUE_POOL = [None, b"", b"x", b"\x00", b"\x00\xff",
              bytes(range(32)), b"v" * 257]
NON_NONE_VALUES = [v for v in VALUE_POOL if v is not None]


def random_rows(rng, n):
    rows = {}
    for _ in range(n):
        key = rng.choice(KEY_POOL)
        if rng.random() < 0.5:
            key += "%05d" % rng.randrange(50)
        rows[key] = rng.choice(NON_NONE_VALUES)
    return rows


def paged(client, snapshot, lo=None, hi=None, limit=3):
    """Full paginated scan; returns (rows, per-page (rows, more) list)."""
    rows, after, pages = [], None, []
    while True:
        page = client.request(snapshot, "scan", lo=lo, hi=hi,
                              after=after, limit=limit)
        pages.append((list(page["rows"]), page["more"]))
        rows.extend(page["rows"])
        if not page["more"]:
            return rows, pages
        assert page["rows"], "empty nonterminal page"
        after = page["rows"][-1][0]


def test_scan_compat(rng):
    """scan stays page-for-page compatible with the frozen reference."""
    rows = random_rows(rng, 400)
    frozen = FrozenReplica(rows)
    cand = Replica(dict(reversed(list(rows.items()))))
    fc, cc = Client(frozen), Client(cand)
    fs, cs = frozen.snapshot(), cand.snapshot()
    bounds = [None, "", "a", "a\x00", "aa", "k000", "k050", "z", "\xff", "水"]
    for _ in range(300):
        lo = rng.choice(bounds)
        hi = rng.choice(bounds)
        if lo is not None and hi is not None and lo.encode() > hi.encode():
            continue
        limit = rng.choice([1, 2, 3, 5, 17, 1024])
        _, f_pages = paged(fc, fs, lo, hi, limit)
        _, c_pages = paged(cc, cs, lo, hi, limit)
        assert f_pages == c_pages, (lo, hi, limit)
    # after-cursor and empty domains behave identically
    for lo, hi, after in [("a", "b", "a"), (None, None, "k050"), ("z", None, "zz"),
                          ("a", "a", None), ("", "", ""), ("水", "水", "a")]:
        limit = 4
        _, f_pages = paged(fc, fs, lo, hi, limit)
        _, c_pages = paged(cc, cs, lo, hi, limit)
        assert f_pages == c_pages, (lo, hi, after, limit)
        f_page = fc.request(fs, "scan", lo=lo, hi=hi, after=after, limit=limit)
        c_page = cc.request(cs, "scan", lo=lo, hi=hi, after=after, limit=limit)
        assert f_page == c_page, (lo, hi, after, limit)
    # invalid inputs raise ValueError on both implementations
    for bad in (0, 1025, -1, True, "3"):
        for client, snap in ((fc, fs), (cc, cs)):
            try:
                client.request(snap, "scan", lo=None, hi=None,
                               after=None, limit=bad)
            except ValueError:
                pass
            else:
                raise AssertionError("bad limit accepted")
    for client, snap in ((fc, fs), (cc, cs)):
        try:
            client.request(snap, "scan", lo="b", hi="a",
                           after=None, limit=1)
        except ValueError:
            pass
        else:
            raise AssertionError("lo > hi accepted")
    # foreign snapshot tokens are rejected
    try:
        cc.request(fs, "scan", lo=None, hi=None, after=None, limit=1)
    except ValueError:
        pass
    else:
        raise AssertionError("foreign snapshot accepted")


def test_diff_random(rng):
    """Randomized batches: diff == dict oracle == frozen linear_diff."""
    lmap = random_rows(rng, 300)
    rmap = random_rows(rng, 300)
    l, r = Replica(lmap), Replica(rmap)
    fl, fr = FrozenReplica(lmap), FrozenReplica(rmap)
    lc, rc = Client(l), Client(r)
    flc, frc = Client(fl), Client(fr)
    lhist = [(l.snapshot(), dict(lmap))]
    rhist = [(r.snapshot(), dict(rmap))]
    flhist = [fl.snapshot()]
    frhist = [fr.snapshot()]
    for step in range(40):
        changes = {}
        for _ in range(rng.randrange(1, 12)):
            changes[rng.choice(KEY_POOL)] = rng.choice(VALUE_POOL)
        if step % 2 == 0:
            lhist.append((l.apply(changes), None))
            flhist.append(fl.apply(changes))
            mutate(lmap, changes)
            lhist[-1] = (lhist[-1][0], dict(lmap))
        else:
            rhist.append((r.apply(changes), None))
            frhist.append(fr.apply(changes))
            mutate(rmap, changes)
            rhist[-1] = (rhist[-1][0], dict(rmap))
        expected = oracle(lmap, rmap)
        assert diff(lc, lhist[-1][0], rc, rhist[-1][0]) == expected
        assert linear_diff(flc, flhist[-1], frc, frhist[-1]) == expected
        # random cross-generation pair, arbitrary retained snapshots
        lt, lm = rng.choice(lhist)
        rt, rm = rng.choice(rhist)
        assert diff(lc, lt, rc, rt) == oracle(lm, rm)


def test_summary_content_not_history(rng):
    """Summaries describe current content, never correction history.

    The same final map reached via a single build and via fragmented
    batches with deletes and reinserts must yield identical summaries over
    identical domains, on both services, whatever the internal layout.
    """
    base = random_rows(rng, 500)
    a = Replica(base)
    b = Replica({})
    keys = list(base)
    rng.shuffle(keys)
    step = max(1, len(keys) // 7)
    for i in range(0, len(keys), step):
        b.apply({k: base[k] for k in keys[i:i + step]})
    noise = {"noise%03d" % i: b"n%d" % i for i in range(30)}
    b.apply(noise)
    b.apply({k: None for k in noise})
    extra = keys[0]
    b.apply({extra: b"temporary"})
    b.apply({extra: base[extra]})
    ac, bc = Client(a), Client(b)
    domains = [[None, None], [None, "k050"], ["k050", None], ["a", "b"],
               ["", "k"], ["水", None], ["a\x00", "aa"]]
    for lo, hi in domains:
        sa = ac.request(a.snapshot(), "summaries", domains=[[lo, hi]])["results"][0]
        sb = bc.request(b.snapshot(), "summaries", domains=[[lo, hi]])["results"][0]
        assert sa["count"] == sb["count"], (lo, hi)
        assert sa["digest"] == sb["digest"], (lo, hi)
    assert diff(ac, a.snapshot(), bc, b.snapshot()) == []


def test_boundaries():
    """Enumerated edge cases against the dict oracle."""
    scenarios = [
        ({}, {}),
        ({}, {"": b""}),
        ({"": b""}, {}),
        ({"\x00": b"x"}, {"": b"y"}),
        ({"a": b"1"}, {"a": b""}),
        ({"aa": b"1", "a": b"2"}, {"a\x00": b"3"}),
        ({"k" * 512: b"v"}, {"k" * 511: b"v"}),
        ({"😀": b"\x00\xff"}, {"😀": b""}),
        ({"a": b"1", "b": b"2", "c": b"3"}, {}),
        ({}, {"\xff": b"hi", "\x00": b"lo"}),
    ]
    for left_rows, right_rows in scenarios:
        l, r = Replica(left_rows), Replica(right_rows)
        got = diff(Client(l), l.snapshot(), Client(r), r.snapshot())
        assert got == oracle(left_rows, right_rows), (left_rows, right_rows)
    # delete everything, then reinsert at both boundaries
    rows = {"": b"0", "a": b"1", "z": b"2"}
    s = Replica(rows)
    e = Replica({})
    original = s.snapshot()
    gone = s.apply({"": None, "a": None, "z": None})
    assert diff(Client(s), gone, Client(e), e.snapshot()) == []
    back = s.apply({"": b"new", "zz": b"max"})
    assert diff(Client(e), e.snapshot(), Client(s), back) == \
        oracle({}, {"": b"new", "zz": b"max"})
    # retained snapshots still answer exactly after later corrections
    assert diff(Client(s), original, Client(s), back) == \
        oracle(rows, {"": b"new", "zz": b"max"})
    assert diff(Client(s), original, Client(s), gone) == oracle(rows, {})


def test_immutable_snapshots(rng):
    """Old snapshots never change as later corrections are applied."""
    rows = random_rows(rng, 200)
    s = Replica(rows)
    c = Client(s)
    tokens = [(s.snapshot(), dict(rows))]
    first_scan, _ = paged(c, tokens[0][0], None, None, 5)
    current = dict(rows)
    for _ in range(15):
        changes = {}
        for _ in range(7):
            changes[rng.choice(KEY_POOL)] = rng.choice(VALUE_POOL)
        mutate(current, changes)
        tokens.append((s.apply(changes), dict(current)))
    again, _ = paged(c, tokens[0][0], None, None, 5)
    assert again == first_scan
    for token, _ in tokens:
        assert diff(c, token, c, token) == []
    for i in range(len(tokens)):
        for j in range(i + 1, len(tokens)):
            if (i + j) % 3 == 0:
                assert diff(c, tokens[i][0], c, tokens[j][0]) == \
                    oracle(tokens[i][1], tokens[j][1])


def test_incremental_sharing():
    """apply() path-copies: few new nodes, old root structurally intact."""
    rows = {"k%05d" % i: bytes([i % 256]) for i in range(3000)}
    s = Replica(rows)
    root_before = s._versions[s.snapshot()]
    before_count, before_total = root_before.count, root_before.total
    changes = {"k00000": b"new", "k01000": None, "k02999": b"x", "new": b"1"}
    token2 = s.apply(changes)
    root_after = s._versions[token2]
    before_ids = set()
    stack = [root_before]
    while stack:
        n = stack.pop()
        before_ids.add(id(n))
        if n.left is not None:
            stack.append(n.left)
        if n.right is not None:
            stack.append(n.right)
    shared = 0
    total_after = 0
    stack = [root_after]
    while stack:
        n = stack.pop()
        total_after += 1
        if id(n) in before_ids:
            shared += 1  # node reused by reference from the old snapshot
        if n.left is not None:
            stack.append(n.left)
        if n.right is not None:
            stack.append(n.right)
    new_nodes = total_after - shared
    # A full rebuild would create ~3000 new nodes; path copying needs
    # only a handful of spines (4 treap operations per changed key).
    assert new_nodes <= len(changes) * 4 * 64, new_nodes
    assert shared >= 3000 - len(changes) * 4 * 64, shared
    assert (root_before.count, root_before.total) == (before_count, before_total)
    assert root_after.count == 3000  # 2 inserts ("new", ...) minus deletes


def test_small_diff_traffic():
    """Small corrections reconcile with a tiny fraction of full-table traffic.

    Uses the same scale as the fixed benchmark (200k records, 20 changes),
    where the contractual budget is defined.
    """
    n = 200000
    rows = {"k%06d" % i: bytes(range(32)) for i in range(n)}
    changes = {"k%06d" % (i * 977): b"c%02d" % i for i in range(20)}
    l, r = Replica(rows), Replica(rows)
    lc, rc = Client(l), Client(r)
    ls = l.snapshot()
    r0 = r.snapshot()
    rs = r.apply(changes)
    lc.reset_meter()
    rc.reset_meter()
    expected_right = dict(rows)
    mutate(expected_right, changes)
    assert diff(lc, ls, rc, rs) == oracle(rows, expected_right)
    optimized_bytes = lc.response_bytes + rc.response_bytes
    fl, fr = FrozenReplica(rows), FrozenReplica(rows)
    flc, frc = Client(fl), Client(fr)
    frs = fr.apply(changes)
    assert linear_diff(flc, fl.snapshot(), frc, frs) == \
        oracle(rows, expected_right)
    reference_bytes = flc.response_bytes + frc.response_bytes
    assert optimized_bytes < reference_bytes * 0.01, \
        (optimized_bytes, reference_bytes)
    # identical snapshots reconcile in O(1) responses, no table pull
    lc.reset_meter()
    rc.reset_meter()
    assert diff(lc, ls, rc, r0) == []
    assert lc.response_bytes + rc.response_bytes < 1024


def main():
    rng = random.Random(20261116)
    test_scan_compat(rng)
    test_diff_random(rng)
    test_summary_content_not_history(rng)
    test_boundaries()
    test_immutable_snapshots(rng)
    test_incremental_sharing()
    test_small_diff_traffic()
    print("verify_extra: all checks passed")


if __name__ == "__main__":
    main()
