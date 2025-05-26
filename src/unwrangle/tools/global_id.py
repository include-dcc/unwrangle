"""
Add global IDs to a dataset file using dewrangle
"""

__name__ = "global-id"
__description__ = "Updates dataset files with global IDs based on the corresponding data-dictionary definitions"

import importlib
import sys
from argparse import FileType
from yaml import safe_load

from csv import DictReader, writer as csv_writer

from ..support import study_descriptors, query_study

_descriptor_details = None


class GlobalVariable:
    def __init__(self, descriptor, resource_type, source_varname, dest_varname):
        self.descriptor = descriptor
        self.resource_type = resource_type
        self.source_varname = source_varname
        self.dest_varname = dest_varname

    def objectify(self):
        return {"descriptor": self.descriptor, "fhirResourceType": self.resource_type}

class GlobalID:
    def __init__(self):
        # Load the descriptor details from our module data
        self.descriptor_details = study_descriptors()

    def extract_ids(self, dd_content, dd_table_name, file_contents):
        """
        Builds a list of descriptors associated within the data in file_contents. 

        Args:
        dd_content (dict): Study variables associated with the global IDs to be generated for the table
        dd_table_name (str): The data-dictionary table name.
        file_contents (DictReader iterable): The study data which for which we are generating global IDs. 

        Returns: 
        list: Object containing "descriptor" and "fhirResourceType" columns suitable for passing to dewrangle
        
        """
        dd_table_details = self.descriptor_details["tables"][dd_table_name]

        # list of ID chunks for each thing contained in this file type
        ids = []
        for global_id in dd_table_details["global_ids"]:
            ids.append(_descriptor_details["global_ids"][global_id])

        descriptors_for_dewrangle = []
        for row in file_contents:
            for id in ids:
                global_varname = id
                descriptor_varname = ids[id]["descriptors"]
                resource_type = ids[id]["resource_type"]
                descriptor = row.get(descriptor_varname)
                global_id = row.get(global_varname)

                if descriptor is not None and descriptor not in dd_content:
                    dd_content[descriptor] = GlobalVariable(
                        descriptor=descriptor,
                        resource_type=resource_type,
                        source_varname=descriptor_varname,
                        dest_varname=global_varname,
                    )
                    descriptors_for_dewrangle.append(dd_content[descriptor].objectify())
        return descriptors_for_dewrangle


    def add_arguments(self, subparsers):
        local_parser = subparsers.add_parser(__name__, help=__description__)
        local_parser.add_argument(
            "-t",
            "--table",
            choices=self.descriptor_details["tables"].keys(),
            help="Specify a data-dictionary to match the file when assigning IDs without a configuration",
        )
        local_parser.add_argument(
            "-f",
            "--file",
            type=FileType("rt"),
            action="append",
            help="Name of file(s) to be updated adhoc. Must specify a data dictionary the file(s) conform to. ",
        )
        local_parser.add_argument(
            "-s",
            "--skip-backup",
            action="store_true",
            help="By default, files are backed up (original_filename_stem.date.extension). This will prevent backups from being created.",
        )


    def exec(self, args):
        if len(args.file) > 0 and len(args.config) > 0:
            sys.stderr.write(
                "Cowardly refusing to process both adhoc files and configs. Please choose one or the other."
            )
            sys.exit(1)

        if len(args.file) > 0 and args.table is None:
            file_list = ",".join(args.files)
            sys.stderr.write(
                f"Unable to process the file(s): {file_list} without a data dictionary."
            )
            sys.exit(1)

        if args.table is not None and len(args.file) == 0:
            sys.stderr.write(
                f"You must provide a file to update that conforms to the selected data-dictionary table."
            )
            sys.exit(1)

        if len(args.file) > 0:
            dd_filename = args.file.name.stem
            variables = {}          # Variables for which IDs were generated
            ids = []
            for f in args.file:
                data_table = DictReader(f)

                # We'll just append them, since they shouldn't be duplicated
                ids += self.extract_ids(variables, f.name, data_table)

