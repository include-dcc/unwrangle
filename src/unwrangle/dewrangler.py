"""
Some functions that are helpful for interacting with the dewrangle application
"""

import importlib
from graphql import print_ast
from gql.transport.aiohttp import AIOHTTPTransport
from gql import Client, gql
import os
import requests

from unwrangle.support import query_study()

_dwtkn = None
_gql_client = None

"""TODO


I'm guessing we will need these properties stored in environment variables 
or configuration (similar to fhir_hosts).

dewrangle: https://dewrangle.com/api
token: blahblah

I feel like tossing it into my home directory is good enough for laptop/desktop 
runs, but we'll probably want to use environment variables inside the docker 
image, presumably provided by github upon deployment
"""



def gql_client(client=None, token=None):
    if client is None:
        if _gql_client is None:
            _gql_client = create_graphql_client(
                get_token(token)
            )
    else:
        _gql_client = client

    return _gql_client


def get_token(token=None):
    if _dwtkn is None:
        _dwtkn = os.environ.get("DEWRANGLE_TOKEN", token)
    return _dwtkn


# based on kf's dewrangle library
def create_graphql_client(token):
    """
    Create a gql GraphQL client that will exec queries asynchronously
    """
    gqlurl = "https://dewrangle.com/api/graphql"

    headers = {"x-api-key": get_token(token)}

    transport = AIOHTTPTransport(url=gqlurl, headers=headers)

    # Create a GraphQL client using the defined transport
    return Client(
        transport=transport,
        fetch_schema_from_transport=True,
        execute_timeout=30,
    )


def get_studies(gql_client=None):
    if gql_client is None:
        gql_client = create_graphql_client()

    query_study = query_study()

    variables = {"first": 10}
    response = gql_client.execute(query_study, variable_values=variables)

    # Build up a data structure for easier access
    studies = {}
    for node in response["viewer"]["organizationUsers"]["edges"]:
        org = node["node"]["organization"]
        org_name = org["name"]
        org_id = org["id"]

        studies[org_name] = {
            "description": org["description"],
            "email": org["email"],
            "id": org["id"],
            "name": org_name,
            "studies": {},
        }

        for study in org["studies"]["edges"]:
            node = study["node"]
            studies[org_name]["studies"][node["name"]] = {
                "id": node["id"],
                "name": node["name"],
                "globalId": node["globalId"],
            }

# {studies['Erics-Test']['studies']['Test Study']['id']}
def update_global_ids(study_id, descriptors):
    headers = {"x-api-key": get_token(), "accept": "application/json"}

    rest_url = f"https://dewrangle.com/api/rest/studies/{study_id}/global-descriptors?descriptors=all"
    for entry in requests.get(rest_url, headers=headers, timeout=None).json():
        assert(entry in descriptors)




graphql_client = create_graphql_client()
