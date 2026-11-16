"""Public diff entry. The baseline transfers every row."""
from frozen import linear_diff


def diff(left, left_snapshot, right, right_snapshot):
    return linear_diff(left, left_snapshot, right, right_snapshot)
