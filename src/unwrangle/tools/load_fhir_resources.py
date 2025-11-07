"""
Upload one or more json files from Whistle that contain FHIR resources into dewrangle
"""
__name__ = "load-fhir-resources"
__description__ = "Loads FHIR resources into dewrangle"


import importlib
import sys
from argparse import FileType
from yaml import safe_load
from collections import defaultdict
from pathlib import Path 

from rich import print

import json

from datetime import datetime
from shutil import copy2 

from ..support.dewrangle import Dewrangler
from csv import DictReader, writer as csv_writer

import logging 
logger = logging.getLogger("unwrangle")
    


def get_study_id(args, config):
    """
    Return the Study ID, which may originate as arg or from config

    :param args: The args from argparse
    :config: Yaml config 
    """
    if args.study_id:
        return args.study_id 
    return config['study_id']    

def add_arguments(subparsers):
    """
    Adds script specific arguments 
    """
    local_parser = subparsers.add_parser(__name__, help=__description__)

    local_parser.add_argument(
        "-pr",
        "--projection",
        choices=["study", "dataset", "harmonized"],
        help="Which projection library is to be used",
    )

    local_parser.add_argument(
        "-f",
        "--file",
        type=FileType("rt"),
        action="append",
        help="Name of file(s) to be updated adhoc. Must specify a data dictionary the file(s) conform to. If processing multiple files, they must all conform to the same data dictionary table.",
    )

def exec(args):
    """
    Loads FHIR resources into dewrangle for a given study ID. 

    User may select multiple JSON files and provide the study id and org or
    they may use a YAML config which provides necessary details. For configs,
    the call must also include the "projection" option so that the application 
    will recognize where to look for the data to be uploaded. 
    """
    if args.file and len(args.file) > 0:
        if args.config is not None and len(args.config) > 0:
            logger.error(
                "Cowardly refusing to process both adhoc files and configs. Please choose one or the other."
            )
            sys.exit(1)
        
        if args.projection is not None:
            logger.warning("Projection settings are ignored when loading files directly.")
        
        if args.study_id is None:
            logger.error("Unable to load resource files without a valid study ID.")
            sys.exit(1)

        args.config = {
            "no-file": {
                "study_id": args.study_id,
                "files": args.file
            }
        }
    else:
        if args.config is None or len(args.config) == 0:
            logger.error(
                f"You must provide a file to update that conforms to the selected data-dictionary table or a study configuration YAML."
            )
            sys.exit(1)

        if args.projection is None:
            logger.error(
                "You must provide a projection type when using a configuration file."
            )
            sys.exit(1)

        for fn, config in args.config.items():
            config['files'] = [open(f"output/whistle-output/{args.projection}/{config['output_filename']}.output.json", 'rt')]


    dw = Dewrangler(args.dwtoken)

    for fn, config in args.config.items():
        fhir_resources = []
        id_issues = {}

        for file in config['files']:
            # Load in each of the resources and append them to the list of resources        
            content = json.load(file)
            skipped = defaultdict(list)
            if type(content) is list:
                fhir_resources += content 
            elif type(content) is dict:
                for module, data in content.items():
                    for resource in data:
                        if "resourceType" in resource and "id" in resource:
                            fhir_resources.append(resource)
                        else:
                            if "resourceType" not in resource:
                                skipped[resource['_no_type']].append(resource)
                            else:
                                skipped[resource['resourceType']].append(resource)

            logger.info(f"{Path(file.name).stem} had {len(content)} resources to be loaded")
            if len(skipped) > 0:
                logger.warning(f"{Path(file.name).stem} had issues with {len(skipped)}")
                for resType, rsrc in skipped.items():
                    logger.warning(f"{resType} - {len(rsrc)} Without Global ID")
                id_issues[Path(file.name).stem] = skipped

        if len(id_issues) > 0:
            Path("output").mkdir(exist_ok=True)
            with open("output/id_issues.json", 'wt') as f:
                json.dump(id_issues, f, ensure_ascii=False, indent=2)

        study_details = dw.study_details(config['study_id'], args.organization_name)
        
        dw.ingest_fhir_resources(study_details['id'], fhir_resources)

