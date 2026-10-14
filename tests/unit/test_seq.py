from sparkplug_pipeline.ingest.seq import SeqStatus, SeqTracker


def observe_all(tracker, seqs):
    return [tracker.observe(s)[0] for s in seqs]


def test_in_order_after_birth():
    tracker = SeqTracker(0)
    assert observe_all(tracker, [1, 2, 3]) == [SeqStatus.IN_ORDER] * 3


def test_wraparound_from_255_to_0_is_in_order():
    tracker = SeqTracker(0)
    statuses = observe_all(tracker, list(range(1, 256)) + [0, 1, 2])
    assert set(statuses) == {SeqStatus.IN_ORDER}
    assert tracker.expected == 3


def test_gap_then_late_arrivals_are_out_of_order():
    tracker = SeqTracker(0)
    observe_all(tracker, [1, 2])
    assert tracker.observe(6) == (SeqStatus.GAP, 3)
    assert tracker.missing == {3, 4, 5}
    assert observe_all(tracker, [4, 3, 5]) == [SeqStatus.OUT_OF_ORDER] * 3
    assert tracker.missing == set()
    assert tracker.observe(7) == (SeqStatus.IN_ORDER, 0)


def test_repeat_of_seen_seq_is_duplicate():
    tracker = SeqTracker(0)
    observe_all(tracker, [1, 2, 3])
    assert tracker.observe(3) == (SeqStatus.DUPLICATE, 0)
    assert tracker.observe(1) == (SeqStatus.DUPLICATE, 0)
    assert tracker.observe(4) == (SeqStatus.IN_ORDER, 0)


def test_gap_across_wraparound():
    tracker = SeqTracker(0)
    observe_all(tracker, range(1, 254))
    assert tracker.observe(1) == (SeqStatus.GAP, 3)
    assert tracker.missing == {254, 255, 0}
    assert tracker.observe(255) == (SeqStatus.OUT_OF_ORDER, 0)


def test_missing_seq_forgotten_when_counter_comes_back_around():
    tracker = SeqTracker(0)
    tracker.observe(2)  # 1 missing
    assert tracker.missing == {1}
    observe_all(tracker, list(range(3, 256)) + [0])
    assert tracker.observe(1) == (SeqStatus.IN_ORDER, 0)
    assert tracker.missing == set()
