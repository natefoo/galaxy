"""Unit tests for install-time data table registration.

These exercise ``InstallRepositoryManager.__handle_repository_contents`` with a real
``ShedToolDataTableManager`` (built against a tmp_path-backed config via the
``make_stdtm`` fixture in ``conftest.py``). Only out-of-scope dependencies
(``InstalledRepositoryMetadataManager``, ``DataManagerHandler``, etc.) are mocked.

Earlier revisions of this test mocked ``ShedToolDataTableManager`` itself and
asserted ``assert_called_once`` against the mock — that meant the very unit under
test was a mock and the test would pass even if the install gating were inverted.
"""

import os
from typing import (
    Any,
)
from unittest import mock

from galaxy.tool_shed.galaxy_install import install_manager

INSTALL_MANAGER_MODULE = "galaxy.tool_shed.galaxy_install.install_manager"


def _build_irm(app):
    """Build a real ``InstallRepositoryManager`` wired to the fixture-built ``app``."""
    irm = install_manager.InstallRepositoryManager.__new__(install_manager.InstallRepositoryManager)
    irm.app = app
    irm.install_model = app.install_model
    # `context` is a read-only property on the real ModelMapping; on this MagicMock
    # stand-in it just needs to exist as a writable attribute so the `session.add(...);
    # session.commit()` calls in __handle_repository_contents land on the mock.
    irm.install_model.context = mock.MagicMock(name="session")  # type: ignore[misc]
    irm.tpm = mock.MagicMock(name="tpm")
    return irm


def _invoke_handle(
    irm,
    repo,
    tool_path: str,
    metadata_dict: dict[str, Any],
    repository_tools_tups: list[Any],
):
    """Invoke the private ``__handle_repository_contents`` with the bare minimum of
    external dependencies stubbed out. The real ``ShedToolDataTableManager`` is
    NOT patched — we want to observe its on-disk effects."""
    irmm_instance = mock.MagicMock(name="irmm_instance")
    irmm_instance.get_metadata_dict.return_value = metadata_dict
    irmm_instance.get_repository_tools_tups.return_value = repository_tools_tups

    patches = [
        mock.patch(f"{INSTALL_MANAGER_MODULE}.InstalledRepositoryMetadataManager", return_value=irmm_instance),
        mock.patch(
            f"{INSTALL_MANAGER_MODULE}.repository_util.get_tool_shed_status_for_installed_repository",
            return_value=None,
        ),
        mock.patch(f"{INSTALL_MANAGER_MODULE}.tool_util.copy_sample_files"),
        mock.patch(
            f"{INSTALL_MANAGER_MODULE}.tool_util.handle_missing_index_file",
            side_effect=lambda *_a, **_k: (repository_tools_tups, []),
        ),
        mock.patch(f"{INSTALL_MANAGER_MODULE}.data_manager.DataManagerHandler"),
    ]
    for p in patches:
        p.start()
    try:
        irm._InstallRepositoryManager__handle_repository_contents(
            tool_shed_repository=repo,
            tool_path=tool_path,
            repository_clone_url="http://tool-shed/repos/owner/name",
            relative_install_dir="",  # repo dir is `tool_path` already
            tool_shed="tool-shed",
            tool_section=None,
            shed_tool_conf=None,
        )
    finally:
        for p in patches:
            p.stop()


def test_non_data_manager_install_writes_loc_file_under_shed(make_stdtm):
    """A non-DM repo with a `.loc.sample` results in a shared loc file under
    ``tool_data_path/shed/`` and a ``<table>`` entry in the shed config."""
    stdtm, repo, samples, captured, tool_data_path, _, _ = make_stdtm()
    irm = _build_irm(stdtm.app)

    metadata = {"sample_files": samples, "tools": [{"id": "t"}]}
    tool_stub = mock.MagicMock()
    tool_stub.params_with_missing_data_table_entry = []
    _invoke_handle(
        irm,
        repo,
        tool_path=repo.get_tool_relative_path.return_value[0],
        metadata_dict=metadata,
        repository_tools_tups=[("p", "guid", tool_stub)],
    )

    assert os.path.exists(os.path.join(tool_data_path, "shed", "all_fasta.loc"))
    # `to_xml_file` was invoked with the new <table> entry.
    assert captured["to_xml_calls"], "Expected stdtm.to_xml_file to be called on first install"


def test_data_manager_install_also_writes_loc_file_under_shed(make_stdtm):
    """Same on-disk effect for a DM repo (the `data_manager` key in metadata only
    drives ``DataManagerHandler``; data-table registration goes through the same path)."""
    stdtm, repo, samples, captured, tool_data_path, _, _ = make_stdtm()
    irm = _build_irm(stdtm.app)

    metadata = {
        "sample_files": samples,
        "tools": [{"id": "t"}],
        "data_manager": {"data_managers": {}},
    }
    tool_stub = mock.MagicMock()
    tool_stub.params_with_missing_data_table_entry = []
    _invoke_handle(
        irm,
        repo,
        tool_path=repo.get_tool_relative_path.return_value[0],
        metadata_dict=metadata,
        repository_tools_tups=[("p", "guid", tool_stub)],
    )

    assert os.path.exists(os.path.join(tool_data_path, "shed", "all_fasta.loc"))


def test_handle_missing_data_table_entry_runs_for_non_dm(make_stdtm):
    """After dropping the `is_data_manager` gate, non-DM repos with a tool whose
    params reference an unregistered data table also get the fallback recovery
    that loads the repo's ``tool_data_table_conf.xml.sample``.

    Mutation check: re-introducing the gate causes this test to fail because the
    in-memory tables are no longer reset (no recovery was attempted)."""
    stdtm, repo, samples, captured, _, _, _ = make_stdtm()
    irm = _build_irm(stdtm.app)
    repo_dir = repo.get_tool_relative_path.return_value[0]

    # Seed an existing table in the registry so we can observe the reset wipe it.
    stdtm.app.tool_data_tables.data_tables = {"sentinel_before_reset": object()}

    tool_stub = mock.MagicMock()
    tool_stub.params_with_missing_data_table_entry = [mock.MagicMock()]  # signals "table missing"

    metadata = {"sample_files": [], "tools": [{"id": "t"}]}  # no install_tool_data_tables run
    _invoke_handle(
        irm, repo, tool_path=repo_dir, metadata_dict=metadata, repository_tools_tups=[("p", "guid", tool_stub)]
    )

    # The fallback called `reset_tool_data_tables()`, which wipes `data_tables`.
    assert (
        stdtm.app.tool_data_tables.data_tables == {}
    ), "Expected handle_missing_data_table_entry to run and reset the in-memory tables"


def test_handle_missing_data_table_entry_no_op_when_no_missing(make_stdtm):
    """The fallback only fires when a tool has `params_with_missing_data_table_entry`;
    otherwise the registry is left untouched."""
    stdtm, repo, samples, captured, _, _, _ = make_stdtm()
    irm = _build_irm(stdtm.app)
    repo_dir = repo.get_tool_relative_path.return_value[0]

    sentinel = object()
    stdtm.app.tool_data_tables.data_tables = {"sentinel": sentinel}

    tool_stub = mock.MagicMock()
    tool_stub.params_with_missing_data_table_entry = []

    metadata = {"sample_files": [], "tools": [{"id": "t"}]}
    _invoke_handle(
        irm, repo, tool_path=repo_dir, metadata_dict=metadata, repository_tools_tups=[("p", "guid", tool_stub)]
    )

    assert stdtm.app.tool_data_tables.data_tables["sentinel"] is sentinel
