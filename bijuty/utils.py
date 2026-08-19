"""
Utility functions and classes for the bijuty package.
"""

from __future__ import annotations

import os
import subprocess
from typing import List, Union, Optional
import logging
import socket


logger = logging.getLogger(__name__)


def run_bash_command(
    cmd: Union[str, List[str]],
    timeout: int = 60,
    shell: bool = False,
) -> subprocess.CompletedProcess:
    """Run a bash command safely and return the result."""
    # Maybe instead of managing shlex.quote multiple times, check it here once.
    # Currently not working.
    # if type(cmd) == list or type(cmd) == List:
    #     safe_cmd = []
    #     for i in cmd:
    #         safe_cmd.append(shlex.quote(i))
    # else:
    #     safe_cmd = shlex.quote(cmd)
    safe_cmd = cmd
    logger.debug(f"Bash command: {cmd}")
    try:
        result = subprocess.run(
            safe_cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=os.environ.copy(),
            shell=shell,
            executable="/bin/bash" if shell else None,
        )
        result.stdout = result.stdout.strip()
        result.stderr = result.stderr.strip()
        return result

    except subprocess.TimeoutExpired as e:
        logger.error(f"Command timed out after {timeout}s: {safe_cmd}")
        stdout = (e.stdout or "").strip()
        stderr = (e.stderr or "").strip() or "Timeout expired"
        return subprocess.CompletedProcess(args=safe_cmd, returncode=124,
                                           stdout=stdout, stderr=stderr)

    except OSError as e:
        logger.error(f"OS error while running command: {e}")
        if isinstance(e, FileNotFoundError):
            returncode, stderr = 127, "Executable not found"
        else:
            returncode, stderr = 1, str(e)
        return subprocess.CompletedProcess(args=safe_cmd, returncode=returncode,
                                           stdout="", stderr=stderr)


def get_file_content(file_path: str):
    """Reads a file and returns its content as a string."""

    try:
        file_path = os.path.abspath(file_path)
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        return f"An error occurred: {e}"


def find_first_available_port(
    self,
    start_port: int = 7077,
    end_port: int = 9000,
    host: Optional[str] = None,
) -> int:
    """Find the first available port in the given range."""
    host = host or socket.gethostname()
    for port in range(start_port, end_port + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((host, port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"No available ports found in range {start_port}-{end_port}")
