"""Unit tests for :mod:`bijuty.templates`."""

from __future__ import annotations

import os
import stat

import pytest

from bijuty import templates


class TestAvailableTemplates:
    def test_lists_spark_and_flink(self):
        assert set(templates.available_templates()) >= {"spark", "flink"}

    def test_returns_lowercase_sorted_names(self):
        names = templates.available_templates()

        assert names == sorted(names)
        assert all(name == name.lower() for name in names)


class TestFactoryTemplatePath:
    @pytest.mark.parametrize("framework", ["spark", "SPARK", "Flink", "flink"])
    def test_returns_existing_directory_case_insensitively(self, framework):
        path = templates.factory_template_path(framework)

        assert os.path.isdir(path)
        assert os.path.basename(os.path.normpath(path)) == framework.lower()

    def test_unknown_framework_raises_value_error(self):
        with pytest.raises(ValueError):
            templates.factory_template_path("hadoop")


class TestInitTemplate:
    def test_copies_factory_tree_to_destination(self, tmp_path):
        destination = tmp_path / "my-flink-template"

        path = templates.init_template("flink", str(destination))

        assert os.path.abspath(path) == str(destination)
        assert (destination / "config.yaml").is_file()
        assert (destination / "meta.conf").is_file()

    def test_preserves_executable_bit_on_scripts(self, tmp_path):
        destination = tmp_path / "spark-template"

        templates.init_template("spark", str(destination))

        mode = os.stat(destination / "cmd.sh").st_mode
        assert mode & stat.S_IXUSR

    def test_expands_user_home(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))

        path = templates.init_template("flink", "~/flink-template")

        assert os.path.isdir(path)
        assert path.startswith(str(tmp_path))

    def test_existing_non_empty_directory_raises_without_overwrite(self, tmp_path):
        destination = tmp_path / "template"
        destination.mkdir()
        (destination / "keep.txt").write_text("keep")

        with pytest.raises(FileExistsError):
            templates.init_template("flink", str(destination))

    def test_overwrite_merges_into_existing_directory(self, tmp_path):
        destination = tmp_path / "template"
        destination.mkdir()
        (destination / "keep.txt").write_text("keep")

        templates.init_template("flink", str(destination), overwrite=True)

        assert (destination / "keep.txt").is_file()
        assert (destination / "config.yaml").is_file()

    def test_existing_empty_directory_is_accepted(self, tmp_path):
        destination = tmp_path / "template"
        destination.mkdir()

        path = templates.init_template("flink", str(destination))

        assert os.path.isdir(path)
        assert (destination / "config.yaml").is_file()

    def test_destination_file_raises_file_exists_error(self, tmp_path):
        destination = tmp_path / "afile"
        destination.write_text("x")

        with pytest.raises(FileExistsError):
            templates.init_template("flink", str(destination), overwrite=True)


class TestMainCli:
    def test_creates_default_destination_and_returns_zero(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.chdir(tmp_path)

        exit_code = templates.main(["flink"])

        assert exit_code == 0
        assert os.path.isdir(tmp_path / "flink-template")
        assert "copied to" in capsys.readouterr().out

    def test_uses_explicit_destination(self, tmp_path):
        destination = tmp_path / "custom"

        exit_code = templates.main(["spark", "--destination", str(destination)])

        assert exit_code == 0
        assert (destination / "spark-defaults.conf").is_file()

    def test_unknown_framework_exits_via_argparse(self):
        with pytest.raises(SystemExit):
            templates.main(["hadoop"])

    def test_errors_are_reported_through_argparse(self, tmp_path):
        destination = tmp_path / "template"
        destination.mkdir()
        (destination / "keep.txt").write_text("keep")

        with pytest.raises(SystemExit):
            templates.main(["flink", "--destination", str(destination)])
