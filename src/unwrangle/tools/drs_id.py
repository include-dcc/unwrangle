__name__ = "drs-id"
__description__ = "TBD -- Updates dataset files with DRS IDs based on the corresponding data-dictionary definitions"


def add_arguments(subparsers):
    local_parser = subparsers.add_parser(__name__, help=__description__)
    local_parser.add_argument("arg", help="TBD")
