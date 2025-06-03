__name__ = "list-global-id"
__description__ = "Updates dataset files with global IDs based on the corresponding data-dictionary definitions"

import importlib
import sys
from argparse import FileType
from yaml import safe_load

from csv import writer as csv_writer
from rich.table import Table as RichTable
from rich.console import Console

from ..support.dewrangle import Dewrangler

def add_arguments(subparsers):
    local_parser = subparsers.add_parser(__name__, help=__description__)
    local_parser.add_argument("-f","--format", choices=['rich', 'csv'], default='rich', help='Format used for output')

def print_csv(descriptors):
    writer = csv_writer(sys.stdout, delimiter=',')

    header = list(descriptors[0].keys())
    writer.writerow(header)
    for item in descriptors:
        writer.writerow([item[x] for x in header])

class ReportLister:
    """Basic lister which will print CSV format to stdout"""
    def __init__(self, header, delimiter=','):
        self.header = header 

        self.writer = csv_writer(sys.stdout, delimiter=delimiter)
        self.writer.writerow(self.header)

    def writerow(self, row):
        self.writer.writerow([row[x] for x in self.header])

class ReportListerPretty:
    """Print to stdout using rich tables"""
    color_list = [
        "cyan",
        "green",
        "blue",
        "red",
        "yellow",
        "magenta",
        "white",
        "bright_cyan",
        "bright_green"
    ]
    def __init__(self, header, title):
        self.header = header
        self.title = title 

        self.writer = RichTable(title=self.title)
        self.console = Console()

        coloridx = 0
        for column in header:
            self.writer.add_column(column, style=ReportListerPretty.color_list[coloridx])
            coloridx += 1

    def writerow(self, row):
        if type(row) is dict:
            self.writer.add_row(*[row[x] for x in self.header])
        else:
            self.writer.add_row(*row)
    
    def __del__(self):
        self.console.print(self.writer)


def exec(args):
    dw = Dewrangler(args.dwtoken)

    if args.config is None and args.study_id is None:
        studies = dw.study_list()

        header = ['Organization', 'Study ID', 'Study Global ID']
        if args.format == "csv":
            writer = ReportLister(header)
        else:
            writer = ReportListerPretty(header, title=f"Study IDs")

        for org in studies:
            for study_id, study in studies[org]['studies'].items():
                writer.writerow([org, study_id, study['globalId']])
        sys.exit(1)


    if args.config:
        study_ids = []
        for k, cfg in args.config.items():
            study_ids = cfg['study_id']
    else:
        study_ids = [args.study_id]


    for id in study_ids:
        study = dw.study_details(id, args.organization_name)
        data = dw.list_descriptors(study['id'])
        header = data[0].keys()

        if args.format == "csv":
            writer = ReportLister(header)
        else:
            writer = ReportListerPretty(header, title=f"Global IDs ({id})")

        for row in data:
            writer.writerow(row)
