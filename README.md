# unwrangle
Python library for interacting with Dewrangler and other ID related systems for use with INCLUDE ETL


## Installation
### Requirements
unwrangle is a set of tools used for ID generation and other interaction with 
the CHOP dewrangle application (some of the IDs are not dewrangle based.)

It is a python CLI application that should work as part of a set of cloud
functions are called directly from a linux shell. 

* Python (Tested on 3.12.6)
* Requests, gql, PyYAML, rich. 

The necessary requirements can be met by simply using pip to install as 
described below. 

### Installation Via Pip
For now, you should clone the repository and install using pip as shown below.

```bash
$ clone https://github.com/include-dcc/unwrangle 
$ cd unwrangle
$ pip install .
```

For developers, you may want to use the -e argument to avoid having to 
reinstall each time you make changes. 

## Usage
As with most linux command line tools, unwrangle supports the -h argument to 
provide help. Because this is tied directly to the code itself, it is generally
up to date. 

```bash 
$ unwrangle -h
usage: unwrangle [-h] [-c CONFIG] [-id STUDY_ID] [-org ORGANIZATION_NAME] {list-global-id,drs-id,global-id} ...

ID Generation assistance for global IDs, DRS IDs, etc

options:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Dataset YAML file with details required to run conversion.
  -id STUDY_ID, --study-id STUDY_ID
                        Study ID (short name). Not required if config is provided.
  -org ORGANIZATION_NAME, --organization-name ORGANIZATION_NAME
                        Organization the study is a part of (not required if user belongs to only one study).

command:
  {list-global-id,drs-id,global-id}
                        Command to be run
    list-global-id      Updates dataset files with global IDs based on the corresponding data-dictionary definitions
    drs-id              TBD -- Updates dataset files with DRS IDs based on the corresponding data-dictionary definitions
    global-id           Updates dataset files with global IDs based on the corresponding data-dictionary definitions
```

For unwrangle, there are subcommands which are the ones that do the actual work. To get the help associated with a given command, you will first need to specify the command itself. 

```bash 
$ unwrangle -h
usage: unwrangle list-global-id [-h] [-f {rich,csv}]

options:
  -h, --help            show this help message and exit
  -f {rich,csv}, --format {rich,csv}
                        Format used for output
```

These arguments are only the subcommand's arguments. You can still provide the unwrangle
arguments in addition. However, be sure to provide those before the command is specified:

```bash
$ unwrangle -id M00M00 list-global-id
Global IDs (M00M00)                                                             ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ globalId      ┃ fhirResourceTy… ┃ descriptor      ┃ descriptorSta… ┃ globalIdCreate… ┃ globalIdCreat… ┃ descriptorCrea… ┃ descriptorCre… ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ dr-9apdf7nzw3 │ DocumentRefere… │ nih-nhlbi-incl… │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ ls-hbzfaiy9g8 │ List            │ M00M00_UH_Demos │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-2inqz2rkhf │ Patient         │ M00_009         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-6egfcfngpz │ Patient         │ M00_003         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-73hnqa3rj3 │ Patient         │ M00_006         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-9fn9yrei4j │ Patient         │ M00_011         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-c2zt2y3773 │ Patient         │ M00_004         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-c8a7nefdy6 │ Patient         │ M00_001         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-h93zmc7tgc │ Patient         │ M00_013         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-iriz8xt6hr │ Patient         │ M00_010         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-k9k92kk8ed │ Patient         │ M00_005         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-kknw9k7tc2 │ Patient         │ M00_014         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-mijtk3eind │ Patient         │ M00_008         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-npyan8mkfk │ Patient         │ M00_002         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-q4p2gqz4j9 │ Patient         │ M00_007         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ pt-ymdb9djznn │ Patient         │ M00_012         │ ACTIVE         │ 2025-06-04T15:… │ eric.s.torste… │ 2025-06-04T15:… │ eric.s.torste… │
│ sd-emfbtamc4g │ ResearchStudy   │                 │                │ 2025-06-04T14:… │ eric.s.torste… │                 │                │
└───────────────┴─────────────────┴─────────────────┴────────────────┴─────────────────┴────────────────┴─────────────────┴────────────────┘
```
Notice that I specified which study ID I wanted id's listed for before I specified my command, list-global-id

The one exception to this is the config which is a positional arg, i.e. arguments which don't include both a flag and a value. Because the command itself is a positional argument, it must come first to be correctly understood by the parser. 

Currently, there are two functional unwrangle commands: 
### list-global-id
This will pull all of the global IDs for a given study. By default, it uses a pretty printer, but can also stream valid CSV contents which the user can redirect to file. 

### global-id
This command accepts either a table and list of filenames or a Whistler config file. The table must be named correctly according to the unwrangle study_descriptor's YAML library configuration. The command will mint IDs for all "global ID" definitions it is aware of for the given table type. For Whistler configurations, it will traverse the entire "dataset" contents to find the dataset's files for the given data-dictionary table. 