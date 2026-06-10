"""
Utility functions and classes for the bijuty package.

This module provides common utilities including logging, environment file handling,
and safe bash command execution.
"""

from __future__ import annotations

import datetime
import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class CommandResult:
    """Result of a command execution."""

    stdout: str
    stderr: str
    returncode: int
    success: bool

    @property
    def failed(self) -> bool:
        """Check if the command failed."""
        return not self.success


# =============================================================================
# Environment Functions
# =============================================================================

def load_env_file(filepath: str) -> None:
    """
    Load environment variables from a file.

    Parses a file containing KEY=VALUE pairs and sets them as environment variables.
    Lines starting with # are treated as comments and ignored.

    Args:
        filepath: Path to the environment file

    Raises:
        FileNotFoundError: If the file doesn't exist
        PermissionError: If there's no permission to read the file
    """
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()


# =============================================================================
# Command Execution
# =============================================================================

def run_bash_command(
    cmd: Union[str, List[str]],
    timeout: int = 60,
    shell: bool = False,
) -> CommandResult:
    """
    Run a bash command safely and return the result.

    This function executes a command with proper error handling and returns
    a structured result containing stdout, stderr, and return code.

    Args:
        cmd: The command to run (string or list of arguments)
        timeout: Maximum time to wait for command completion (seconds)
        shell: Whether to run the command through the shell

    Returns:
        CommandResult containing stdout, stderr, returncode, and success status

    Examples:
        >>> result = run_bash_command("echo hello")
        >>> if result.success:
        ...     print(result.stdout)

        >>> result = run_bash_command(["ls", "-la"], timeout=30)
    """
    current_env = os.environ.copy()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
            env=current_env,
            shell=shell,
            executable="/bin/bash" if shell else None,
        )
        return CommandResult(
            stdout=result.stdout.strip(),
            stderr="",
            returncode=0,
            success=True,
        )

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.strip() or e.stdout.strip()
        logger.error(f"Command failed: {cmd}\nError: {error_output}")
        return CommandResult(
            stdout=e.stdout.strip(),
            stderr=error_output,
            returncode=e.returncode,
            success=False,
        )

    except subprocess.TimeoutExpired as e:
        logger.error(f"Command timed out after {timeout}s: {cmd}")
        stdout = e.stdout.decode().strip() if e.stdout else ""
        stderr = e.stderr.decode().strip() if e.stderr else "Timeout expired"
        return CommandResult(
            stdout=stdout,
            stderr=stderr,
            returncode=124,
            success=False,
        )

    except FileNotFoundError:
        logger.error(f"Executable not found: {cmd if isinstance(cmd, str) else cmd[0]}")
        return CommandResult(
            stdout="",
            stderr="Executable not found",
            returncode=127,
            success=False,
        )

    except OSError as e:
        logger.error(f"OS error while running command: {e}")
        return CommandResult(
            stdout="",
            stderr=str(e),
            returncode=1,
            success=False,
        )

def get_file_content(file_path):
    """Reads a file and returns its content as a string."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        return "Error: The file was not found."
    except Exception as e:
        return f"An error occurred: {e}"

def debug_write_to_file(content="here", file_path="./tmp.txt"):
    with open(file_path, "a") as f:
        f.write(content)
        f.write("\n")