"""Prevent CI from installing the pre-extraction model dependency graph."""

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]


def test_lock_matches_current_transport_project():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    package = next(item for item in lock["package"] if item["name"] == project["name"])
    assert package["version"] == project["version"]
    assert lock["requires-python"] == project["requires-python"].replace(",", ", ")
    assert {dep["name"] for dep in package["dependencies"]} == {"grpcio", "protobuf", "pyarrow"}
    assert not {"repast4py", "mpi4py", "torch"}.intersection(item["name"] for item in lock["package"])


def test_ci_checks_lock_freshness_before_installing():
    action = (ROOT / ".github/actions/setup-python-env/action.yml").read_text()
    assert "uv sync --locked" in action
    assert "uv sync --frozen" not in action
