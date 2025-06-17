"""
Add global IDs to a dataset file using dewrangle
"""

__name__ = "global-id"
__description__ = "Updates dataset files with global IDs based on the corresponding data-dictionary definitions"

import importlib
import sys
from argparse import FileType
from yaml import safe_load

from collections import defaultdict
from pathlib import Path 

from datetime import datetime
from shutil import copy2 

from ..support.dewrangle import Dewrangler
from csv import DictReader, writer as csv_writer

from ..support import study_descriptors# , query_study

_global_ids = None

from rich import print

import pdb

class MissingDescriptor(Exception):
    def __init__(self, table, descriptors, global_var):
        self.table = table 
        self.descriptors = ", ".join(descriptors)
        self.global_var = global_var 

    def message(self):
        return f"Incomplete descriptor information for '{self.global_var} from table, '{self.table}. Columns in question: {self.descriptors}"

    def __str__(self):
        return self.message()

    def __str__(self):
        return self.message()
class MissingDestinationColumn(Exception):
    def __init__(self, table, global_var):
        self.table = table 
        self.global_var = global_var 
    
    def message(self):
        return f"Unable to write global ID to column, [blue]'{self.table}:{self.global_var}'[/blue], because that column doesn't exit."
class GlobalVariable:
    def __init__(self, descriptor, resource_type, source_varnames, dest_varname, global_id=None):
        self.descriptor = descriptor
        self.resource_type = resource_type
        self.source_varnames = source_varnames
        self.dest_varname = dest_varname
        self.global_id = global_id

    def objectify(self):
        return {"descriptor": self.descriptor, "fhirResourceType": self.resource_type, "dest_varname": self.dest_varname}



class GlobalID:
    # print(study_descriptors().keys())
    default_descriptor_delimiter = study_descriptors()['default_descriptor_delimiter']

    def __init__(self, variable_name, descriptors, resource_type, state=None, descriptor_delimiter=None):
        self.variable_name = variable_name
        self.descriptors = descriptors 
        self.resource_type = resource_type 
        self.descriptor_delimiter = descriptor_delimiter 
        self.descriptor_state = state

        if self.descriptor_delimiter is None:
            self.descriptor_delimiter = GlobalID.default_descriptor_delimiter 

        # descriptor => GlobalVariable
        self.variables = {}
        self.reported_descriptor_issues = defaultdict(set)

    def global_id(self, row):
        return self.variables[self.build_descriptor(row)].global_id

    def build_descriptor(self, row):
        desc_components = [row.get(x) for x in self.descriptors]

        if None in desc_components:
            raise MissingDescriptor(self.resource_type, self.descriptors, self.variable_name)
        return self.descriptor_delimiter.join(desc_components)
    
    def add_variable(self, descriptor, resource_type, global_id=None):
        if global_id == "TBD":
            global_id=None
        if descriptor not in self.variables:
            self.variables[descriptor] = GlobalVariable(
                descriptor=descriptor, 
                resource_type=resource_type, 
                source_varnames = self.descriptors, 
                dest_varname = self.variable_name, 
                global_id = global_id 
            )
        else:
            if global_id is not None and global_id:
                if self.variables[descriptor].global_id is not None:
                    assert self.variables[descriptor].global_id == global_id, f"Trying to replace global ID, {self.variables[descriptor].global_id}, with {global_id} for descriptor, {descriptor}. Cowardly refusing such a thing!"
                self.variables[descriptor].global_id = global_id 

        return self.variables[descriptor]
    
    def parse_row(self, row):
        # print(row)
        try:
            descriptor = self.add_variable(
                descriptor = self.build_descriptor(row),
                resource_type = self.resource_type,
                global_id=row.get(self.variable_name)
            )
        except MissingDescriptor as e:
            descriptor = None
            error = str(e)

            if error not in self.reported_descriptor_issues[e.table]:
                print(e)
                self.reported_descriptor_issues[e.table].add(error)
        return descriptor

def extract_descriptors(ids_of_interest, csvfile):
    """Adds descriptor details to be sent to dewrangle only when the descriptor\
       is valid and the global ID doesn't already exist. 
    """
    descriptors = []
    descriptors_observed = set()
    for row in csvfile:
        for global_id in ids_of_interest:
            global_var = global_id.parse_row(row)
            if global_var:
                if global_var.dest_varname in row:
                    if global_var.descriptor is not None and (global_var.global_id is None or global_var.global_id.strip() in ['', 'TBD']):
                        if global_var.descriptor.strip() != "" and global_var.descriptor not in descriptors_observed:
                            descriptors_observed.add(global_var.descriptor)
                            descriptors.append(global_var.objectify())
                else:
                    e = MissingDestinationColumn(table=global_var.resource_type, global_var=global_var.dest_varname)
                    error = str(e)
                    if error not in global_var.reported_descriptor_issues[e.table]:
                        print(error)
                        global_var.reported_descriptor_issues[e.table].add(error)
    return descriptors

def collect_ids_for_files(dw, org, study_id, table, filelist, backupdir):
    # Filename => Number of lines changed
    lines_updated = defaultdict(int)

    #pdb.set_trace()
    if table in study_descriptors()['tables']:
        # Just a quick lookup for the global ID objects
        if study_descriptors()['tables'][table].get("global_ids") is not None:
            global_id_list = set(study_descriptors()['tables'][table]['global_ids'])
            ids_of_interest = []

            # resource_type => { descriptor => global_id }
            global_ids = defaultdict(dict)

            for global_id, item in study_descriptors()['global_ids'].items():
                id = GlobalID(variable_name = global_id,
                            descriptors=item.get('descriptors'),
                            resource_type=item.get('resource_type'),
                            descriptor_delimiter=item.get('descriptor_delimiter')
                            )
                
                if global_id in global_id_list:
                    ids_of_interest.append(id)
            
                global_ids[id.resource_type][global_id] = id 

            study_details = dw.study_details(study_id, org)
            for descriptor in dw.list_descriptors(study_details['id']):
                global_ids[descriptor['fhirResourceType']][descriptor['descriptor']] = descriptor['globalId']
                
            # At this point, we have all of the global IDs that have previously been 
            # created. 
            # pdb.set_trace()
            for filename in filelist:
                print(f"\n\nMinting Global IDs for {study_id}:{filename} ({table})")
                descriptors_for_dewrangle = []
                existing_descriptors = {}

                with Path(filename).open('rt') as file:
                    csvfile = DictReader(file, delimiter=',')

                    # Extract ID descriptors from dataset file for rows where global IDs 
                    # are missing and we do have the data to generate a descriptor
                    descriptors_of_interest = extract_descriptors(ids_of_interest, csvfile)
                    for descr in descriptors_of_interest:
                        if descr['descriptor'] in global_ids[descr['fhirResourceType']]:
                            existing_descriptors[descr['descriptor']] = global_ids[descr['fhirResourceType']][descr['descriptor']]
                        else:
                            descriptors_for_dewrangle.append(descr)

                # If there are global IDs to mint:
                if len(descriptors_for_dewrangle) + len(existing_descriptors) > 0:
                    #   Back up file
                    if backupdir is not None:
                        Path(backupdir).mkdir(parents=True, exist_ok=True)  # / datetime.now().strftime("%Y%m%d%H%M%S") 
                        backup_filename = Path(backupdir) / (Path(filename).stem + ".csv")
                        copy2(filename, backupdir)

                    if len(descriptors_for_dewrangle) > 0:
                        # 
                        #   Mint Global IDs and merge the new global IDs into our global_ids
                        #   data structure
                        id_response = dw.update_descriptors(study_details['id'], descriptors_for_dewrangle)
                        ids_returned = 0
                        for id, response in id_response.items():
                            global_ids[response['fhirResourceType']][response['descriptor']] = response['globalId']
                            ids_returned += 1
                    
                        print(f"- {ids_returned} out of {len(descriptors_for_dewrangle)} were returned.")
                    if len(existing_descriptors) > 0:
                        print(f"- Updating {len(existing_descriptors)} ids from pre-existing IDs")
                    
                    missing_descriptors = 0
                    with backup_filename.open('rt') as infile:
                        print(f"* Backup filename: {backup_filename}")
                        #pdb.set_trace()
                        csvinput = DictReader(infile, delimiter=',')
                        header = csvinput.fieldnames
                        # 
                        #   Update the CSVs with global IDs and continue
                        print(f"- Writing updates to {filename}")
                        with Path(filename).open('wt') as outf:
                            csvfile = csv_writer(outf, delimiter=',')
                            dewrangle_new = 0
                            dewrangle_preexisting = 0

                            csvfile.writerow(header)
                            for row in csvinput:
                                # pdb.set_trace()
                                for id in ids_of_interest:
                                    if row.get(id.variable_name) is None or row.get(id.variable_name) in ['', 'TBD']:
                                        try:
                                            local_descriptor = id.build_descriptor(row)
                                            # pdb.set_trace()
                                            if local_descriptor in existing_descriptors:
                                                row[id.variable_name] = existing_descriptors[local_descriptor]
                                                dewrangle_preexisting += 1
                                            else:
                                                # pdb.set_trace()
                                                if local_descriptor not in ['', 'TBD']:
                                                    row[id.variable_name] = global_ids[id.resource_type][local_descriptor]
                                                    dewrangle_new += 1
                                                else:
                                                    print(row)
                                                    print(f"The descriptor for {id.variable_name} resulted in an invalid descriptor")
                                        except MissingDescriptor as e:
                                            missing_descriptors += 1
                                csvfile.writerow([row[x] for x in header])
                                lines_updated[Path(filename).stem] += 1
                        print(f"* File updated with {dewrangle_new} global IDs from dewrangle and {dewrangle_preexisting} previously generated")
                        if missing_descriptors > 0:
                            print(f"* {missing_descriptors} IDs could not be generated due to the absence of one or more descriptor columns")
    else:
        sys.stderr.write(f"No global ID information in the study_descriptor configuration for table, {table}.\n")
        sys.stderr.write(f"Available table names: \n\t* {'\n\t* '.join(study_descriptors()['tables'].keys())}")
    return lines_updated

def add_arguments(subparsers):
    local_parser = subparsers.add_parser(__name__, help=__description__)

    local_parser.add_argument(
        "config", 
        type=FileType("rt"),
        nargs='*',
        help="1 or more Whistler Study YAML files"
    )
    local_parser.add_argument(
        "-t",
        "--table",
        choices=study_descriptors()["tables"].keys(),
        help="Specify a data-dictionary to match the file when assigning IDs without a configuration",
    )
    local_parser.add_argument(
        "-f",
        "--file",
        type=FileType("rt"),
        action="append",
        help="Name of file(s) to be updated adhoc. Must specify a data dictionary the file(s) conform to. If processing multiple files, they must all conform to the same data dictionary table.",
    )
    local_parser.add_argument(
        "-b",
        "--backup-directory", 
        type=str,
        default='backup',
        help="Directory where backup copies are written. "
    )
    local_parser.add_argument(
        "-s",
        "--skip-backup",
        action="store_true",
        help="By default, files are backed up (original_filename_stem.date.extension). This will prevent backups from being created.",
    )


def exec(args):
    #pdb.set_trace()
    if args.file and len(args.file) > 0:
        if args.config is not None and len(args.config) > 0:
            sys.stderr.write(
                "Cowardly refusing to process both adhoc files and configs. Please choose one or the other."
            )
            sys.exit(1)

        if args.table is None:
            file_list = ",".join(args.files)
            sys.stderr.write(
                f"Unable to process the file(s): {file_list} without a data dictionary (table)."
            )
            sys.exit(1)
    else:
        if args.config is None or len(args.config) == 0:
            sys.stderr.write(
                f"You must provide a file to update that conforms to the selected data-dictionary table or a study configuration YAML."
            )
            sys.exit(1)
    dw = Dewrangler(args.dwtoken)

    if args.config:
        for config_filename, config in args.config.items():
            if args.organization_name is None and config['organization'] is not None:
                args.organization_name = config['organization']
                print(f"[green]Pulling Organization from config: {args.organization_name}[/green]")
            # pdb.set_trace()
            study_id = config['study_id']
            backupdir = Path(args.backup_directory) / f"{study_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            #study_details = dw.study_details(study_id)
            for table, ddset in config['dataset'].items():
                if args.table is None or table == args.table:
                    # pdb.set_trace()
                    if args.table is None or table == args.table:
                        updates_made = collect_ids_for_files(dw, args.organization_name, study_id, table, ddset['filename'].split(","), backupdir)
                        for update in updates_made:
                            print(f"{table}\t{update}\t{updates_made[update]} lines updated")
    else:
        study_id = args.study_id
        if study_id is None:
            sys.stderr.write(f"Runs without a config must contain the Study ID associated with the global IDs to be generated")
            sys.exit(1)
        backupdir = Path(args.backup_directory) / f"{study_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        updates_made = collect_ids_for_files(dw, args.organization_name, study_id, args.table, [f.name for f in args.file], backupdir=backupdir)
        for update in updates_made:
            print(f"{args.table}\t{update}\t{updates_made[update]} lines updated")

