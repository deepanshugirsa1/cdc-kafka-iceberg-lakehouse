import pytest

from bench.benchmark import summarize

BATCHES = [
    {"batch_id": 0, "rows": 0, "started": 0.0, "ended": 0.5},
    {"batch_id": 1, "rows": 1000, "started": 1.0, "ended": 3.0},
    {"batch_id": 2, "rows": 3000, "started": 3.0, "ended": 6.0},
]


def test_throughput_uses_wall_clock_across_active_batches():
    report = summarize(BATCHES)
    assert report["merged_rows"] == 4000
    assert report["wall_seconds"] == 5.0
    assert report["events_per_sec_end_to_end"] == 800.0


def test_empty_batches_do_not_depress_the_number():
    assert summarize(BATCHES)["batches"] == 2


def test_latency_percentiles_are_reported():
    report = summarize(BATCHES)
    assert report["batch_latency_p50_s"] == 2.5
    assert report["max_batch_rows"] == 3000


def test_no_rows_is_an_error_not_a_zero():
    with pytest.raises(SystemExit):
        summarize([{"batch_id": 0, "rows": 0, "started": 0.0, "ended": 1.0}])
