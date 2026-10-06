"""Unit tests for :mod:`bijuty.utils`."""

from __future__ import annotations

import os
import subprocess
from unittest.mock import patch

import pytest

from bijuty.utils import (
    find_first_available_port,
    get_file_content,
    run_bash_command,
)


class TestRunBashCommand:
    """Tests for :func:`run_bash_command`."""

    def test_run_bash_command_list_success_strips_output(self):
        result = run_bash_command(["printf", "  hello  "], shell=False)

        assert result.returncode == 0
        assert result.stdout == "hello"
        assert result.stderr == ""

    def test_run_bash_command_shell_pipeline_success(self):
        result = run_bash_command("echo hello | tr a-z A-Z", shell=True)

        assert result.returncode == 0
        assert result.stdout == "HELLO"

    def test_run_bash_command_nonzero_returncode_is_preserved(self):
        result = run_bash_command(["ls", "/definitely/not/here"], shell=False)

        assert result.returncode != 0
        assert result.stderr

    def test_run_bash_command_missing_executable_returns_127(self):
        result = run_bash_command(
            ["bijuty-command-that-does-not-exist"], shell=False)

        assert result.returncode == 127
        assert result.stderr == "Executable not found"

    @pytest.mark.parametrize("timeout", [1])
    def test_run_bash_command_timeout_returns_124(self, timeout):
        result = run_bash_command(["sleep", "5"], timeout=timeout)

        assert result.returncode == 124
        assert result.stderr == "Timeout expired"
        assert isinstance(result, subprocess.CompletedProcess)

    def test_run_bash_command_generic_oserror_returns_returncode_1(self):
        with patch("bijuty.utils.subprocess.run", side_effect=OSError("boom")):
            result = run_bash_command(["whatever"], shell=False)

        assert result.returncode == 1
        assert result.stderr == "boom"
        assert result.stdout == ""

    def test_run_bash_command_passes_environment_copy(self):
        with patch("bijuty.utils.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr="")
            run_bash_command(["true"], shell=False)

        _, kwargs = run.call_args
        assert kwargs["env"] is not os.environ
        assert kwargs["env"]["PATH"] == os.environ["PATH"]


class TestGetFileContent:
    """Tests for :func:`get_file_content`."""

    def test_get_file_content_reads_utf8_file(self, tmp_path):
        target = tmp_path / "sample.txt"
        target.write_text("héllo wörld", encoding="utf-8")

        assert get_file_content(str(target)) == "héllo wörld"

    def test_get_file_content_resolves_relative_paths(self, tmp_path, monkeypatch):
        target = tmp_path / "rel.txt"
        target.write_text("relative", encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        assert get_file_content("rel.txt") == "relative"

    def test_get_file_content_missing_file_returns_error_string(self, tmp_path):
        result = get_file_content(str(tmp_path / "missing.txt"))

        assert result.startswith("An error occurred:")


class TestFindFirstAvailablePort:
    """Tests for :func:`find_first_available_port`."""

    def test_returns_first_bindable_port(self):
        with patch("bijuty.utils.socket.socket") as socket_cls:
            sock = socket_cls.return_value.__enter__.return_value
            sock.bind.return_value = None

            port = find_first_available_port(
                start_port=6000, end_port=6002, host="example")

        assert port == 6000
        sock.bind.assert_called_once_with(("example", 6000))

    def test_skips_ports_that_are_in_use(self):
        with patch("bijuty.utils.socket.socket") as socket_cls:
            sock = socket_cls.return_value.__enter__.return_value
            sock.bind.side_effect = [OSError("busy"), OSError("busy"), None]

            port = find_first_available_port(
                start_port=7000, end_port=7002, host="example")

        assert port == 7002
        assert sock.bind.call_count == 3

    def test_raises_runtime_error_when_no_port_available(self):
        with patch("bijuty.utils.socket.socket") as socket_cls:
            sock = socket_cls.return_value.__enter__.return_value
            sock.bind.side_effect = OSError("busy")

            with pytest.raises(RuntimeError, match="No available ports"):
                find_first_available_port(
                    start_port=8000, end_port=8001, host="example")

    def test_defaults_host_to_local_hostname(self):
        with patch("bijuty.utils.socket.socket") as socket_cls, \
                patch("bijuty.utils.socket.gethostname",
                      return_value="myhost") as gethostname:
            sock = socket_cls.return_value.__enter__.return_value
            sock.bind.return_value = None

            find_first_available_port(start_port=9000, end_port=9000)

        gethostname.assert_called_once()
        assert sock.bind.call_args[0][0][0] == "myhost"
