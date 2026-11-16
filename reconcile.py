"""Summary-guided exact reconciler.

For each half-open key domain [lo, hi) the reconciler requests the content
summary (record count + SHA256 content digest) of *both* snapshots over
exactly the same domain. Equal summaries skip the whole domain without
transferring records; unequal domains are bisected at the median key of
the larger side; domains holding at most _LEAF records on both sides are
fetched in full and merged exactly. The two services may partition their
indexes arbitrarily: summaries are shape-independent and only ever
compared over identical domains of their own snapshots. Every read goes
through the metered wire.Client; the full table is never pulled.
"""

_LEAF = 8


def _merge_rows(lrows, rrows):
    """Exact merge of two key-ordered [[key, value_hex], ...] row lists."""
    out = []
    i = j = 0
    n, m = len(lrows), len(rrows)
    while i < n or j < m:
        if j >= m or (i < n and lrows[i][0].encode("utf-8")
                      < rrows[j][0].encode("utf-8")):
            out.append((lrows[i][0], bytes.fromhex(lrows[i][1]), None))
            i += 1
        elif i >= n or rrows[j][0].encode("utf-8") < lrows[i][0].encode("utf-8"):
            out.append((rrows[j][0], None, bytes.fromhex(rrows[j][1])))
            j += 1
        else:
            if lrows[i][1] != rrows[j][1]:
                out.append((lrows[i][0], bytes.fromhex(lrows[i][1]),
                            bytes.fromhex(rrows[j][1])))
            i += 1
            j += 1
    return out


def diff(left, left_snapshot, right, right_snapshot):
    out = []
    frontier = [(None, None)]
    while frontier:
        domains = [[lo, hi] for lo, hi in frontier]
        lres = left.request(left_snapshot, "summaries",
                            domains=domains)["results"]
        rres = right.request(right_snapshot, "summaries",
                             domains=domains)["results"]
        next_frontier = []
        fetches = []
        for (lo, hi), lsum, rsum in zip(frontier, lres, rres):
            if lsum["count"] == rsum["count"] \
                    and lsum["digest"] == rsum["digest"]:
                continue  # same domain, same snapshot-bound content: skip
            if lsum["count"] <= _LEAF and rsum["count"] <= _LEAF:
                fetches.append((lo, hi))
                continue
            mid = lsum["mid"] if lsum["count"] >= rsum["count"] \
                else rsum["mid"]
            if mid is None or (lo is not None and mid <= lo) \
                    or (hi is not None and mid >= hi):
                raise AssertionError("invalid split key")
            next_frontier.append((lo, mid))
            next_frontier.append((mid, hi))
        if fetches:
            domains = [[lo, hi] for lo, hi in fetches]
            lrows = left.request(left_snapshot, "fetch",
                                 domains=domains)["results"]
            rrows = right.request(right_snapshot, "fetch",
                                  domains=domains)["results"]
            for lpage, rpage in zip(lrows, rrows):
                out.extend(_merge_rows(lpage, rpage))
        frontier = next_frontier
    out.sort(key=lambda item: item[0].encode("utf-8"))
    return out
