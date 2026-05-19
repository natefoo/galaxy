"""Unit tests for shed-side data table install behavior."""

import logging
import os
from unittest import mock

import pytest

from galaxy.tool_shed.tools.data_table_manager import (
    _parse_table_columns,
    DataTableColumnMismatch,
)
from galaxy.tool_util.data import TabularToolDataTable
from galaxy.util import (
    Element,
    SubElement,
)


def _write(path: str, contents: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(contents)


def _registered_table(columns, filenames=None):
    existing = mock.MagicMock(spec=TabularToolDataTable)
    existing.columns = columns
    existing.filenames = filenames or {}
    existing.parse_file_fields.return_value = []
    return existing


def _registered_non_tabular(columns, filenames=None):
    """Plain MagicMock with no spec so ``isinstance(mock, TabularToolDataTable)`` is
    False — used to verify the isinstance gate in ``_merge_loc_sample_entries`` skips
    non-tabular table types (e.g. refgenie)."""
    existing = mock.MagicMock()
    existing.columns = columns
    existing.filenames = filenames or {}
    return existing


def _write_shed_config_with_entry(stdtm, table_name, file_path):
    """Pre-populate ``shed_tool_data_table_config`` with a single ``<table>``."""
    shed_config = stdtm.app.config.shed_tool_data_table_config
    contents = f"""<?xml version="1.0"?>
<tables>
    <table name="{table_name}" comment_char="#">
        <columns>value, dbkey, name, path</columns>
        <file path="{file_path}" />
    </table>
</tables>
"""
    with open(shed_config, "w") as fh:
        fh.write(contents)


def test_loc_file_lands_under_shed_subdir_not_per_revision(make_stdtm):
    stdtm, repo, samples, captured, tool_data_path, shed_tool_data_path, _ = make_stdtm()
    _, kept_elems = stdtm.install_tool_data_tables(repo, samples)

    shared_loc = os.path.join(tool_data_path, "shed", "all_fasta.loc")
    assert os.path.exists(shared_loc)
    # The loc file does NOT land at the tool_data_path root (which is for admin-configured loc
    # files) — Galaxy-managed shed loc files are isolated under tool_data_path/shed/.
    assert not os.path.exists(os.path.join(tool_data_path, "all_fasta.loc"))
    per_rev_loc = os.path.join(shed_tool_data_path, "owner/name/abc", "all_fasta.loc")
    assert not os.path.exists(per_rev_loc)

    assert len(kept_elems) == 1
    file_elems = list(kept_elems[0].findall("file"))
    assert len(file_elems) == 1
    assert file_elems[0].get("path") == shared_loc
    assert kept_elems[0].find("tool_shed_repository") is None


def test_existing_loc_file_is_not_overwritten(make_stdtm):
    stdtm, repo, samples, _, tool_data_path, _, _ = make_stdtm()
    shared_loc = os.path.join(tool_data_path, "shed", "all_fasta.loc")
    os.makedirs(os.path.dirname(shared_loc), exist_ok=True)
    _write(shared_loc, "preexisting DM-populated content\n")

    stdtm.install_tool_data_tables(repo, samples)

    with open(shared_loc) as fh:
        assert fh.read() == "preexisting DM-populated content\n"


def test_column_mismatch_raises(make_stdtm):
    stdtm, repo, samples, captured, _, _, _ = make_stdtm()
    stdtm.app.tool_data_tables.data_tables = {
        "all_fasta": _registered_table({"value": 0, "name": 1, "path": 2}),
    }

    with pytest.raises(DataTableColumnMismatch) as exc_info:
        stdtm.install_tool_data_tables(repo, samples)
    assert exc_info.value.table_name == "all_fasta"
    assert not captured["to_xml_calls"]


def test_column_match_first_install_writes_table_entry(make_stdtm):
    """When a table is already registered in memory but has not yet been written to
    ``shed_tool_data_table_config``, we still write a (stamp-less) ``<table>`` entry so the
    shared loc file's association with the table survives reload."""
    stdtm, repo, samples, captured, tool_data_path, _, _ = make_stdtm()
    matching_columns = {"value": 0, "dbkey": 1, "name": 2, "path": 3}
    stdtm.app.tool_data_tables.data_tables = {
        "all_fasta": _registered_table(matching_columns),
    }

    _, kept_elems = stdtm.install_tool_data_tables(repo, samples)

    assert len(kept_elems) == 1
    file_elems = list(kept_elems[0].findall("file"))
    assert len(file_elems) == 1
    assert file_elems[0].get("path") == os.path.join(tool_data_path, "shed", "all_fasta.loc")
    assert kept_elems[0].find("tool_shed_repository") is None


def test_column_match_subsequent_install_dedupes_shed_config_entry(make_stdtm):
    """If shed_tool_data_table_config already has a ``<table>`` with the same name and same
    ``<file path>``, don't write another one."""
    stdtm, repo, samples, captured, tool_data_path, _, _ = make_stdtm()
    matching_columns = {"value": 0, "dbkey": 1, "name": 2, "path": 3}
    stdtm.app.tool_data_tables.data_tables = {
        "all_fasta": _registered_table(matching_columns),
    }
    _write_shed_config_with_entry(stdtm, "all_fasta", os.path.join(tool_data_path, "shed", "all_fasta.loc"))

    _, kept_elems = stdtm.install_tool_data_tables(repo, samples)

    assert kept_elems == []
    assert not captured["to_xml_calls"]


def test_dedup_does_not_match_same_name_different_file_paths(make_stdtm):
    """Same table name but different `<file path>` must NOT dedupe — the entries refer
    to distinct loc files and both should be persisted."""
    stdtm, repo, samples, captured, tool_data_path, _, _ = make_stdtm()
    matching_columns = {"value": 0, "dbkey": 1, "name": 2, "path": 3}
    stdtm.app.tool_data_tables.data_tables = {
        "all_fasta": _registered_table(matching_columns),
    }
    # Existing entry points to a completely different loc file.
    _write_shed_config_with_entry(stdtm, "all_fasta", "/some/other/path/all_fasta.loc")

    _, kept_elems = stdtm.install_tool_data_tables(repo, samples)

    assert len(kept_elems) == 1, "Different file paths should not be deduped"
    file_elem = kept_elems[0].find("file")
    assert file_elem.get("path") == os.path.join(tool_data_path, "shed", "all_fasta.loc")


def test_column_match_with_column_elements_writes_entry(make_stdtm):
    stdtm, repo, _, captured, tool_data_path, _, _ = make_stdtm()
    column_form_conf = """\
<tables>
    <table name="all_fasta" comment_char="#">
        <column name="value" index="0" />
        <column name="dbkey" index="1" />
        <column name="name" index="2" />
        <column name="path" index="3" />
        <file path="tool-data/all_fasta.loc" />
    </table>
</tables>
"""
    repo_dir, _ = repo.get_tool_relative_path.return_value
    _write(os.path.join(repo_dir, "tool_data_table_conf.xml.sample"), column_form_conf)
    stdtm.app.tool_data_tables.data_tables = {
        "all_fasta": _registered_table({"value": 0, "dbkey": 1, "name": 2, "path": 3}),
    }

    _, kept_elems = stdtm.install_tool_data_tables(
        repo,
        ["tool_data_table_conf.xml.sample", os.path.join("tool-data", "all_fasta.loc.sample")],
    )
    assert len(kept_elems) == 1
    assert kept_elems[0].find("tool_shed_repository") is None


def test_second_install_merges_loc_sample_rows_with_attribution(make_stdtm):
    stdtm, repo, samples, captured, tool_data_path, _, _ = make_stdtm()
    matching_columns = {"value": 0, "dbkey": 1, "name": 2, "path": 3}
    existing = _registered_table(matching_columns)
    incoming_rows = [["hg19", "hg19", "human (hg19)", "/data/hg19.fa"]]
    existing.parse_file_fields.return_value = incoming_rows
    stdtm.app.tool_data_tables.data_tables = {"all_fasta": existing}
    _write_shed_config_with_entry(stdtm, "all_fasta", os.path.join(tool_data_path, "shed", "all_fasta.loc"))

    _, kept_elems = stdtm.install_tool_data_tables(repo, samples)

    assert kept_elems == []
    assert not captured["to_xml_calls"]
    existing.append_entries_with_attribution.assert_called_once()
    call_args = existing.append_entries_with_attribution.call_args
    assert call_args.args[0] == incoming_rows
    attribution = call_args.args[1]
    assert "iuc/data_manager_fetch_genome_dbkeys_all_fasta" in attribution
    assert "abc" in attribution


def test_second_install_with_empty_loc_sample_does_not_append(make_stdtm):
    stdtm, repo, samples, captured, tool_data_path, _, _ = make_stdtm()
    matching_columns = {"value": 0, "dbkey": 1, "name": 2, "path": 3}
    existing = _registered_table(matching_columns)
    existing.parse_file_fields.return_value = []
    stdtm.app.tool_data_tables.data_tables = {"all_fasta": existing}
    _write_shed_config_with_entry(stdtm, "all_fasta", os.path.join(tool_data_path, "shed", "all_fasta.loc"))

    _, kept_elems = stdtm.install_tool_data_tables(repo, samples)

    assert kept_elems == []
    assert not captured["to_xml_calls"]
    existing.append_entries_with_attribution.assert_not_called()


def test_non_tabular_table_skips_merge_and_dedup(make_stdtm):
    """If the already-registered table isn't a ``TabularToolDataTable``, both the
    ``.loc.sample`` merge and the shed-config dedup check are skipped — we write a
    fresh ``<table>`` entry and never call the table's row-parsing APIs (which
    would be wrong for non-tabular formats like refgenie's YAML)."""
    stdtm, repo, samples, _, tool_data_path, _, _ = make_stdtm()
    existing = _registered_non_tabular({"value": 0, "dbkey": 1, "name": 2, "path": 3})
    stdtm.app.tool_data_tables.data_tables = {"all_fasta": existing}
    _write_shed_config_with_entry(stdtm, "all_fasta", os.path.join(tool_data_path, "shed", "all_fasta.loc"))

    _, kept_elems = stdtm.install_tool_data_tables(repo, samples)
    assert len(kept_elems) == 1, "Expected a fresh <table> entry since dedup is skipped for non-tabular"
    existing.parse_file_fields.assert_not_called()
    existing.append_entries_with_attribution.assert_not_called()


def test_merge_skips_rows_with_here_token(make_stdtm, caplog):
    """Rows in a ``.loc.sample`` that reference ``${__HERE__}`` are dropped with a
    warning: the shared loc lives under ``tool_data_path/shed/`` so a preserved
    ``${__HERE__}`` would resolve to the wrong directory."""
    stdtm, repo, samples, _, tool_data_path, _, _ = make_stdtm()
    matching_columns = {"value": 0, "dbkey": 1, "name": 2, "path": 3}
    existing = _registered_table(matching_columns)
    existing.parse_file_fields.return_value = [
        ["hg19", "hg19", "Human", "${__HERE__}/hg19.fa"],
        ["mm10", "mm10", "Mouse", "/abs/path/mm10.fa"],
    ]
    stdtm.app.tool_data_tables.data_tables = {"all_fasta": existing}
    _write_shed_config_with_entry(stdtm, "all_fasta", os.path.join(tool_data_path, "shed", "all_fasta.loc"))

    with caplog.at_level(logging.WARNING, logger="galaxy.tool_shed.tools.data_table_manager"):
        stdtm.install_tool_data_tables(repo, samples)

    # The HERE row was dropped; the absolute-path row was appended.
    existing.append_entries_with_attribution.assert_called_once()
    appended = existing.append_entries_with_attribution.call_args.args[0]
    assert appended == [["mm10", "mm10", "Mouse", "/abs/path/mm10.fa"]]
    assert any("__HERE__" in rec.message for rec in caplog.records)


def test_parse_table_columns_aliases_name_to_value():
    elem = Element("table")
    cols = SubElement(elem, "columns")
    cols.text = "value, path"
    parsed = _parse_table_columns(elem)
    assert parsed == {"value": 0, "path": 1, "name": 0}
