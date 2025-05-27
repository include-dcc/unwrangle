__name__ = "list-global-id"
__description__ = "Updates dataset files with global IDs based on the corresponding data-dictionary definitions"

import importlib
import sys
from argparse import FileType
from yaml import safe_load

from csv import writer as csv_writer

from ..support.dewrangle import Dewrangler

def add_arguments(subparsers):
    local_parser = subparsers.add_parser(__name__, help=__description__)
    local_parser.add_argument("-f","--format", choices=['csv'], default='csv', help='Format used for output')

def print_csv(descriptors):
    writer = csv_writer(sys.stdout, delimiter=',')

    header = list(descriptors[0].keys())
    writer.writerow(header)
    for item in descriptors:
        writer.writerow([item[x] for x in header])

def exec(args):
    dw = Dewrangler(args.dwtoken)

    if args.config is None and args.study_id is None:
        studies = dw.study_list()
        writer = csv_writer(sys.stdout, delimiter=',')

        header = ['Organization', 'Study ID', 'Study Global ID']
        writer.writerow(header)
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

        print_csv(dw.list_descriptors(study['id']))
