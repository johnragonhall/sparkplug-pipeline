"""Per-node Sparkplug sequence number tracking.

Every message an edge node sends after NBIRTH carries ``seq``, which counts
0..255 and wraps. NBIRTH itself carries seq 0. The tracker compares each seq
with the next expected value and classifies it:

* IN_ORDER: the expected value.
* GAP: ahead of the expected value; the skipped numbers are remembered as
  missing.
* OUT_OF_ORDER: behind the expected value and one of the remembered missing
  numbers, so a message that was overtaken has now arrived.
* DUPLICATE: behind the expected value and not missing, so it was already seen.

"Ahead" and "behind" use half of the 256 range: a seq up to 127 steps ahead of
the expected value counts as a gap, anything else counts as behind. A missing
number is forgotten when the counter comes back around to it in order.
"""

from enum import Enum

SEQ_MODULUS = 256
_HALF = SEQ_MODULUS // 2


class SeqStatus(Enum):
    IN_ORDER = "in_order"
    GAP = "gap"
    OUT_OF_ORDER = "out_of_order"
    DUPLICATE = "duplicate"


class SeqTracker:
    __slots__ = ("expected", "missing")

    def __init__(self, birth_seq: int = 0) -> None:
        self.expected = (birth_seq + 1) % SEQ_MODULUS
        self.missing: set[int] = set()

    def observe(self, seq: int) -> tuple[SeqStatus, int]:
        """Classify ``seq``. The second value is the number of skipped seqs for GAP, else 0."""
        diff = (seq - self.expected) % SEQ_MODULUS
        if diff == 0:
            self.missing.discard(seq)
            self.expected = (seq + 1) % SEQ_MODULUS
            return SeqStatus.IN_ORDER, 0
        if diff < _HALF:
            for step in range(diff):
                self.missing.add((self.expected + step) % SEQ_MODULUS)
            self.missing.discard(seq)
            self.expected = (seq + 1) % SEQ_MODULUS
            return SeqStatus.GAP, diff
        if seq in self.missing:
            self.missing.remove(seq)
            return SeqStatus.OUT_OF_ORDER, 0
        return SeqStatus.DUPLICATE, 0
