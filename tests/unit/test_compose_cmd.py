import pytest

from tests.integration.compose_cmd import compose_command


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("docker", ["docker", "compose"]),
        ("docker-compose", ["docker-compose"]),
        ("wsl", ["wsl", "-e", "docker", "compose"]),
    ],
)
def test_allowed_names(name, expected):
    assert compose_command(name) == expected


@pytest.mark.parametrize(
    "name",
    ["docker compose; whoami", "sh -c id", "wsl -d Ubuntu -e sh", "", "DOCKER"],
)
def test_other_values_are_refused(name):
    with pytest.raises(ValueError, match="not an allowed compose command"):
        compose_command(name)
