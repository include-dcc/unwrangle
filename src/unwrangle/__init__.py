from pathlib import Path
from argparse import ArgumentParser, FileType
import sys

# import wstlr
import wstlr.config  # import Configuration
from wstlr import get_host_config

from yaml import safe_load

from unwrangle.tools import load_tools

import pdb

tools = load_tools()


def exec(args=None):
    # host_config = get_host_config()
    # env_options = sorted(host_config.keys())

    parser = ArgumentParser(
        prog="unwrangle",
        description="""ID Generation assistance for global IDs, DRS IDs, etc""",
    )
    """
    parser.add_argument(
        "--host",
        choices=env_options,
        help=f"Remote configuration to be used to access the FHIR server. If no environment is provided, the system will stop after generating the whistle output (no validation, no loading)",
    )
    parser.add_argument(
        "-e",
        "--env",
        choices=["local", "dev", "qa", "prod"],
        help=f"If your config has host details configured, you can use these short cuts to choose the appropriate host details. This is useful if you wish to run different configurations on the same command, but each has a different target host. ",
    )"""
    parser.add_argument(
        "-c",
        "--config",
        action="append",
        type=FileType("rt"),
        help="Dataset YAML file with details required to run conversion.",
    )

    subparsers = parser.add_subparsers(
        title="command", dest="command", required=True, help="Command to be run"
    )
    for toolname in tools:
        tools[toolname].add_arguments(subparsers)

    args = parser.parse_args(args)

    # Now, we run the command the user selected
    tools[args.command].exec(args)
