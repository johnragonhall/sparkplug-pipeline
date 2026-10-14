"""Sparkplug B topic parsing.

Topic layout (Sparkplug B v3.0.0):

    spBv1.0/<group_id>/<message_type>/<edge_node_id>[/<device_id>]
    spBv1.0/STATE/<host_id>
"""

from dataclasses import dataclass

NAMESPACE = "spBv1.0"
STATE_PREFIX = f"{NAMESPACE}/STATE/"

NODE_TYPES = frozenset({"NBIRTH", "NDEATH", "NDATA", "NCMD"})
DEVICE_TYPES = frozenset({"DBIRTH", "DDEATH", "DDATA", "DCMD"})


class TopicError(ValueError):
    """Raised when a topic is not a valid Sparkplug B edge topic."""


@dataclass(frozen=True, slots=True)
class Topic:
    group_id: str
    message_type: str
    edge_node: str
    device: str | None = None

    def __str__(self) -> str:
        parts = [NAMESPACE, self.group_id, self.message_type, self.edge_node]
        if self.device is not None:
            parts.append(self.device)
        return "/".join(parts)


def is_state_topic(topic: str) -> bool:
    return topic.startswith(STATE_PREFIX)


def parse_topic(topic: str) -> Topic:
    """Split an edge topic into its parts, or raise TopicError."""
    parts = topic.split("/")
    if parts[0] != NAMESPACE:
        raise TopicError(f"namespace is {parts[0]!r}, expected {NAMESPACE!r}")
    if len(parts) < 4:
        raise TopicError(f"expected at least 4 levels, got {len(parts)}")
    _, group_id, message_type, edge_node, *rest = parts
    if message_type in NODE_TYPES:
        if rest:
            raise TopicError(f"{message_type} topic must not name a device")
        device = None
    elif message_type in DEVICE_TYPES:
        if len(rest) != 1:
            raise TopicError(f"{message_type} topic must name exactly one device")
        device = rest[0]
    else:
        raise TopicError(f"unknown message type {message_type!r}")
    for label, value in (("group_id", group_id), ("edge_node", edge_node), ("device", device)):
        if value is not None and (not value or "+" in value or "#" in value):
            raise TopicError(f"{label} {value!r} is empty or contains a wildcard")
    return Topic(group_id, message_type, edge_node, device)
