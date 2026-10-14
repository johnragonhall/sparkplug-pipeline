import pytest

from sparkplug_pipeline.sparkplug.topic import Topic, TopicError, is_state_topic, parse_topic


def test_parse_device_topic():
    t = parse_topic("spBv1.0/plant1/DDATA/edge-01/press-01")
    assert t == Topic("plant1", "DDATA", "edge-01", "press-01")
    assert str(t) == "spBv1.0/plant1/DDATA/edge-01/press-01"


def test_parse_node_topic():
    assert parse_topic("spBv1.0/plant1/NBIRTH/edge-01") == Topic("plant1", "NBIRTH", "edge-01")


def test_state_topic_is_recognized():
    assert is_state_topic("spBv1.0/STATE/ingest-01")
    assert not is_state_topic("spBv1.0/plant1/NDATA/edge-01")


@pytest.mark.parametrize(
    "topic",
    [
        "spAv1.0/plant1/DDATA/edge-01/press-01",
        "spBv1.0/plant1/DDATA/edge-01",
        "spBv1.0/plant1/NDATA/edge-01/press-01",
        "spBv1.0/plant1/XDATA/edge-01",
        "spBv1.0/plant1",
        "spBv1.0//NBIRTH/edge-01",
        "spBv1.0/plant1/DDATA/edge-01/a/b",
    ],
)
def test_invalid_topics_raise(topic):
    with pytest.raises(TopicError):
        parse_topic(topic)
