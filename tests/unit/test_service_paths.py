import pytest

from sparkplug_pipeline.ingest.service import confined_path


def test_relative_name_resolves_inside_base(tmp_path):
    assert confined_path("ingest-ready", tmp_path) == (tmp_path / "ingest-ready").resolve()


def test_absolute_path_inside_base_is_accepted(tmp_path):
    target = tmp_path / "sub" / "ready"
    assert confined_path(str(target), tmp_path) == target.resolve()


@pytest.mark.parametrize("value", ["../escape", "sub/../../escape", "."])
def test_traversal_is_rejected(tmp_path, value):
    with pytest.raises(ValueError, match="outside"):
        confined_path(value, tmp_path / "base")


def test_absolute_path_elsewhere_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="outside"):
        confined_path(str(tmp_path.parent / "elsewhere"), tmp_path)
