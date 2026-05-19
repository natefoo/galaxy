import os
from unittest import mock

import pytest

from galaxy.tool_shed.tools.data_table_manager import ShedToolDataTableManager
from galaxy.tool_util.data import ToolDataTableManager
from galaxy.util import (
    parse_xml,
    xml_to_string,
)
from tool_shed.webapp.model import (
    Repository,
    User,
)
from ._util import (
    provides_repositories_fixture,
    random_name,
    repository_fixture,
    TestToolShedApp,
    user_fixture,
)

SAMPLE_TABLE_CONF = """\
<tables>
    <table name="all_fasta" comment_char="#">
        <columns>value, dbkey, name, path</columns>
        <file path="tool-data/all_fasta.loc" />
    </table>
</tables>
"""

LOC_SAMPLE_CONTENT = "# all_fasta.loc sample\n"


class _FakeTableRegistry(ToolDataTableManager):
    """In-memory stand-in for ``ToolDataTableManager`` used by shed-install unit tests.

    ``to_xml_file`` writes the supplied ``new_elems`` to the target so dedup checks
    that re-read the file (e.g. ``_load_shed_config_tree``) round-trip correctly.
    """

    def __init__(self):
        self.data_tables: dict = {}
        self.to_xml_calls: list = []

    def to_xml_file(self, shed_tool_data_table_config, new_elems=None, remove_elems=None):
        self.to_xml_calls.append((shed_tool_data_table_config, new_elems))
        existing_root = None
        if os.path.exists(shed_tool_data_table_config):
            try:
                existing_root = parse_xml(shed_tool_data_table_config).getroot()
            except Exception:
                existing_root = None
        with open(shed_tool_data_table_config, "wb") as fh:
            fh.write(b'<?xml version="1.0"?>\n<tables>\n')
            if existing_root is not None:
                for child in existing_root:
                    fh.write(xml_to_string(child).encode("utf-8"))
                    fh.write(b"\n")
            for elem in new_elems or []:
                fh.write(xml_to_string(elem).encode("utf-8"))
                fh.write(b"\n")
            fh.write(b"</tables>\n")

    def add_new_entries_from_config_file(
        self, config_filename, tool_data_path, shed_tool_data_table_config, persist=False
    ):
        # Tests don't need to exercise the real loader; record the call and return empty.
        return [], None


def _write_file(path: str, contents: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(contents)


@pytest.fixture
def make_stdtm(tmp_path):
    """Build a ``ShedToolDataTableManager`` against a ``tmp_path``-backed config.

    Returns a callable ``factory()`` that yields:
        ``(stdtm, repo, sample_files, captured, tool_data_path,
            shed_tool_data_path, relative_target_dir)``
    where ``captured["to_xml_calls"]`` lets the test observe shed-config writes.
    """

    def _factory():
        repo_dir = str(tmp_path / "repo")
        tool_data_path = str(tmp_path / "tool-data")
        shed_tool_data_path = str(tmp_path / "shed_tool_data")
        shed_tool_data_table_config = str(tmp_path / "shed_data_table_conf.xml")
        relative_target_dir = "owner/name/abc"

        os.makedirs(tool_data_path, exist_ok=True)
        os.makedirs(os.path.join(shed_tool_data_path, relative_target_dir), exist_ok=True)
        _write_file(os.path.join(repo_dir, "tool_data_table_conf.xml.sample"), SAMPLE_TABLE_CONF)
        _write_file(os.path.join(repo_dir, "tool-data", "all_fasta.loc.sample"), LOC_SAMPLE_CONTENT)

        app = mock.MagicMock(name="app")
        app.config.tool_data_path = tool_data_path
        app.config.shed_tool_data_path = shed_tool_data_path
        app.config.shed_tool_data_table_config = shed_tool_data_table_config
        registry = _FakeTableRegistry()
        app.tool_data_tables = registry
        captured = {"to_xml_calls": registry.to_xml_calls}

        stdtm = ShedToolDataTableManager(app)

        repo = mock.MagicMock(name="tool_shed_repository")
        repo.name = "data_manager_fetch_genome_dbkeys_all_fasta"
        repo.owner = "iuc"
        repo.installed_changeset_revision = "abc"
        repo.tool_shed = "tool-shed"
        repo.get_tool_relative_path.return_value = (repo_dir, relative_target_dir)

        sample_files = [
            "tool_data_table_conf.xml.sample",
            os.path.join("tool-data", "all_fasta.loc.sample"),
        ]
        return stdtm, repo, sample_files, captured, tool_data_path, shed_tool_data_path, relative_target_dir

    return _factory


@pytest.fixture
def shed_app():
    app = TestToolShedApp()
    yield app


@pytest.fixture
def new_user(shed_app: TestToolShedApp) -> User:
    return user_fixture(shed_app, random_name())


@pytest.fixture
def new_repository(shed_app: TestToolShedApp, new_user: User) -> Repository:
    return repository_fixture(shed_app, new_user, random_name())


@pytest.fixture
def provides_repositories(shed_app: TestToolShedApp, new_user: User) -> User:
    return provides_repositories_fixture(shed_app, new_user)
