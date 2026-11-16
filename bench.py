"""Fixed business workloads. Initialization is measured separately."""
import gc
import hashlib
import json
from statistics import median
import sys
from time import perf_counter
from checks import implementation, mutate, oracle
from wire import Client

N = 200000
ROUNDS = 3
BATCHES = 3
TIME_RATIO = 0.25
BYTE_RATIO = 0.01


def fixture():
    return {"k%06d" % i: hashlib.sha256(("record:%d" % i).encode()).digest()
            for i in range(N)}


def workload(kind):
    batches = []
    for batch in range(BATCHES):
        if kind == "spread":
            changes = {"k%06d" % ((j * 9973 + batch * 29) % N):
                       hashlib.sha256(("correction:%d:%d" % (batch, j)).encode()).digest()
                       for j in range(20)}
        elif kind == "boundary":
            changes = {"k%06d" % (batch * 7 + j): None for j in range(7)}
            for j in range(7):
                key = "" if batch == 0 and j == 0 else "k%06d\x00%d" % (batch * 7 + j, batch)
                changes[key] = b"insert:" + bytes([batch, j])
            for j in range(6):
                changes["k%06d" % (N - 1 - j)] = b"revision:" + bytes([batch, j])
        else:
            raise ValueError(kind)
        assert len(changes) == 20
        batches.append(changes)
    return batches


def once(mode, rows, batches):
    Replica, diff = implementation(mode)
    started = perf_counter()
    l, r = Replica(rows), Replica(dict(reversed(list(rows.items()))))
    lc, rc = Client(l), Client(r)
    ls, rs = l.snapshot(), r.snapshot()
    init_seconds = perf_counter() - started
    expected_left, expected_right = dict(rows), dict(rows)
    work_seconds = 0.0
    response_bytes = 0
    for changes in batches:
        lc.reset_meter()
        rc.reset_meter()
        started = perf_counter()
        rs = r.apply(changes)
        result = diff(lc, ls, rc, rs)
        work_seconds += perf_counter() - started
        response_bytes += lc.response_bytes + rc.response_bytes
        mutate(expected_right, changes)
        assert result == oracle(expected_left, expected_right)
    return {"init_seconds": init_seconds, "work_seconds": work_seconds,
            "response_bytes": response_bytes}


def run(mode):
    if mode not in ("baseline", "target"):
        raise ValueError(mode)
    started = perf_counter()
    rows = fixture()
    fixture_seconds = perf_counter() - started
    output = {"records": N, "rounds": ROUNDS, "batches_per_round": BATCHES,
              "changes_per_batch": 20, "fixture_seconds": fixture_seconds,
              "workloads": {}}
    for kind in ("spread", "boundary"):
        batches = workload(kind)
        runs = {"reference": [], "candidate": []}
        for round_no in range(ROUNDS):
            order = ("reference", "candidate") if round_no % 2 == 0 else ("candidate", "reference")
            for label in order:
                gc.collect()
                runs[label].append(once("reference" if label == "reference" else mode, rows, batches))
        ref = median(x["work_seconds"] for x in runs["reference"])
        candidate = median(x["work_seconds"] for x in runs["candidate"])
        ratios = [b["response_bytes"] / a["response_bytes"]
                  for a, b in zip(runs["reference"], runs["candidate"])]
        summary = {"reference": runs["reference"], "candidate": runs["candidate"],
                   "median_time_ratio": candidate / ref,
                   "round_byte_ratios": ratios}
        output["workloads"][kind] = summary
    # Report all measured results even when optimization budgets are not met.
    if mode == "target":
        print(json.dumps({"measurements": output}, sort_keys=True), flush=True)
        for summary in output["workloads"].values():
            assert summary["median_time_ratio"] <= TIME_RATIO, "time budget"
            assert all(x <= BYTE_RATIO for x in summary["round_byte_ratios"]), "response budget"
    return output


if __name__ == "__main__":
    print(json.dumps(run(sys.argv[1]), sort_keys=True))
