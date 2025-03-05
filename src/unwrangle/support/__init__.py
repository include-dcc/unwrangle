""" 
Helper functions for support files, such as graphql queries and mutations, YAML data, etc. 
"""

import importlib
from graphql import print_ast
from gql.transport.aiohttp import AIOHTTPTransport
from gql import Client, gql

from yaml import safe_load


_support_details = (
    importlib.resources.files("unwrangle") / "support"
)  # descriptors.yaml

# GraphQL query associated with gathering studies we can access
_query_study = None

# Mapping the descriptor to the output global ID column.
_study_descriptors = None


def query_study():
    global _query_study
    if _query_study is None:
        _query_study = gql((_support_details / "query_studies.gql").read_text())
    return _query_study


def study_descriptors():
    global _study_descriptors

    if _study_descriptors is None:
        _study_descriptors = safe_load(
            open((_support_details / "study_descriptors.yaml").read_text())
        )
    return _study_descriptors
