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

