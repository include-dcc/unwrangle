from pathlib import Path
import importlib

import pdb

import sys


# First, let's collect all of our tools so that we can add them to our kit

# The root dir is the current module directory where we will have one or
# more tools that we can execute.
rootdir = Path(__file__).absolute().parent / ".."


def load_tools():
    tools = {}

    # Anything inside the tools directory that starts with a letter is going
    # to be recognized as a tool, so don't put anything in there named like
    # a tool that doesn't exhibit the tool interface. 
    for filename in Path(__file__).absolute().parent.glob("[A-Za-z]*.py"):
        toolname = filename.stem
        tool_lib = importlib.import_module(f"unwrangle.tools.{toolname}")
        tools[tool_lib.__name__] = tool_lib
    return tools
