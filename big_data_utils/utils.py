"""
Utility functions and classes for the big_data_utils package.

This module provides common utilities including logging, environment file handling,
and safe bash command execution.
"""

from __future__ import annotations

import datetime
import enum
import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union


# =============================================================================
# Enums
# =============================================================================

class LogLevel(enum.Enum):
    """Logging levels for the SimpleLogger."""

    INFO = "INFO "
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    WARNING = "WARN "


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
# Logging
# =============================================================================

class SimpleLogger:
    """
    A simple logger that prints messages with timestamps and log levels.

    This logger provides basic logging functionality without external dependencies.
    It prints to stdout with formatted timestamps and colored log levels.
    """

    # ANSI color codes
    COLORS = {
        LogLevel.INFO: "\033[94m",      # Blue
        LogLevel.DEBUG: "\033[90m",     # Gray
        LogLevel.ERROR: "\033[91m",     # Red
        LogLevel.WARNING: "\033[93m",   # Yellow
        "reset": "\033[0m",
    }

    def __init__(self, use_colors: bool = True):
        """
        Initialize the logger.

        Args:
            use_colors: Whether to use ANSI color codes in output
        """
        self.use_colors = use_colors

    def log(self, message: str, level: LogLevel) -> None:
        """
        Log a message with the specified level.

        Args:
            message: The message to log
            level: The log level
        """
        timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        if self.use_colors:
            color = self.COLORS.get(level, "")
            reset = self.COLORS["reset"]
            print(f"[{color}{level.value}{reset}] [{timestamp}] - {message}")
        else:
            print(f"[{level.value}] [{timestamp}] - {message}")

    def info(self, message: str) -> None:
        """Log an info message."""
        self.log(message, LogLevel.INFO)

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.log(message, LogLevel.DEBUG)

    def error(self, message: str) -> None:
        """Log an error message."""
        self.log(message, LogLevel.ERROR)

    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.log(message, LogLevel.WARNING)


# Global logger instance
mylogger = SimpleLogger()


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
        mylogger.error(f"Command failed: {cmd}\nError: {error_output}")
        return CommandResult(
            stdout=e.stdout.strip(),
            stderr=error_output,
            returncode=e.returncode,
            success=False,
        )

    except subprocess.TimeoutExpired as e:
        mylogger.error(f"Command timed out after {timeout}s: {cmd}")
        stdout = e.stdout.decode().strip() if e.stdout else ""
        stderr = e.stderr.decode().strip() if e.stderr else "Timeout expired"
        return CommandResult(
            stdout=stdout,
            stderr=stderr,
            returncode=124,
            success=False,
        )

    except FileNotFoundError:
        mylogger.error(f"Executable not found: {cmd if isinstance(cmd, str) else cmd[0]}")
        return CommandResult(
            stdout="",
            stderr="Executable not found",
            returncode=127,
            success=False,
        )

    except OSError as e:
        mylogger.error(f"OS error while running command: {e}")
        return CommandResult(
            stdout="",
            stderr=str(e),
            returncode=1,
            success=False,
        )
