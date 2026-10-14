"""Selects the compose command for the integration test from a fixed set.

SPB_COMPOSE names one of the commands below. Each branch returns a literal
argument list, so the environment variable picks a command but never supplies
any part of it.
"""

ALLOWED = ("docker", "docker-compose", "wsl")


def compose_command(name: str) -> list[str]:
    """Return the argument list for ``name``, or raise ValueError for anything else."""
    if name == "docker":
        return ["docker", "compose"]
    if name == "docker-compose":
        return ["docker-compose"]
    if name == "wsl":
        # Docker Engine inside the default WSL distribution, for Windows hosts.
        return ["wsl", "-e", "docker", "compose"]
    raise ValueError(
        f"SPB_COMPOSE={name!r} is not an allowed compose command; "
        f"use one of: {', '.join(ALLOWED)}"
    )
