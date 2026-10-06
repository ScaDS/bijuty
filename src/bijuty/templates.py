"""Create editable copies of the bundled ("factory") framework configuration templates."""

from __future__ import annotations

import argparse
import os
import shutil
from typing import List, Optional

from .gui.config import FRAMEWORK_REGISTRY


def available_templates() -> List[str]:
    """Return the lowercase names of frameworks that ship a factory template."""
    return sorted(
        config.name_lower
        for config in FRAMEWORK_REGISTRY.values()
        if os.path.isdir(config.default_template)
    )


def factory_template_path(framework: str) -> str:
    """Return the bundled factory template directory for ``framework``."""
    name = framework.upper()
    if name not in FRAMEWORK_REGISTRY:
        options = ", ".join(sorted(FRAMEWORK_REGISTRY))
        raise ValueError(
            f"Unknown framework '{framework}'. Expected one of: {options}."
        )

    template = FRAMEWORK_REGISTRY[name].default_template
    if not os.path.isdir(template):
        raise FileNotFoundError(
            f"No factory template found for '{framework}' at '{template}'."
        )
    return template


def init_template(
    framework: str,
    destination: str,
    overwrite: bool = False,
) -> str:
    """Copy a factory template to ``destination`` so it can be edited."""
    source = factory_template_path(framework)
    target = os.path.abspath(os.path.expanduser(destination))

    if os.path.exists(target) and not os.path.isdir(target):
        raise FileExistsError(
            f"Destination '{target}' exists and is not a directory."
        )

    if os.path.isdir(target) and os.listdir(target) and not overwrite:
        raise FileExistsError(
            f"Destination '{target}' already exists and is not empty. "
            "Pass overwrite=True to merge into it."
        )

    shutil.copytree(source, target, dirs_exist_ok=True)
    return target


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point for the ``bijuty-template`` command."""
    parser = argparse.ArgumentParser(
        prog="bijuty-template",
        description=(
            "Create an editable copy of a BiJuTy factory framework "
            "configuration template."
        ),
    )
    parser.add_argument(
        "framework",
        type=str.lower,
        choices=available_templates(),
        help="Framework whose factory template should be copied.",
    )
    parser.add_argument(
        "-d",
        "--destination",
        default=None,
        help="Directory to create the template in (default: ./<framework>-template).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Merge into the destination if it already exists.",
    )
    args = parser.parse_args(argv)

    destination = args.destination or f"./{args.framework}-template"
    try:
        path = init_template(
            args.framework, destination, overwrite=args.overwrite)
    except (ValueError, FileNotFoundError, FileExistsError) as error:
        parser.error(str(error))

    print(f"Factory template '{args.framework}' copied to: {path}")
    print(
        "Edit the files, then load this directory via the Configuration Panel "
        "(uncheck 'Use default template') and start the cluster."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
