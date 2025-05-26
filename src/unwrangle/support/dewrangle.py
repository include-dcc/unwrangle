# Support for dewrangle queries


from graphql import print_ast
from gql.transport.aiohttp import AIOHTTPTransport
from gql import Client, gql
import requests
from . import _support_details 

class Dewrangler:
    def __init__(self, token, dwrest="https://dewrangle.com/api/rest", gqurl="https://dewrangle.com/api/graphql" ):
        self.client = None


        assert token is not None, "Dewrangle must be given a valid dewrangle token!"
        # Token associated with the dewrangle client access
        self.token = token
        self.gqurl = gqurl          # Dewrangle GraphQL URL
        self.dwrest = dwrest        # REST Api URL 

        # GraphQL query associated with gathering studies we can access
        self.study_query = None

        self.studies = None 

    def study_query(self):
        if self.study_query is None:
            self.study_query = gql((_support_details / "query_studies.gql").read_text())
        return self.study_query

    def descriptor_upsert(self):
        if self.descriptor_upsert is None:
            self.descriptor_upsert = gql((_support_details / "descriptor_upsert.gql").read_text())
        return self.descriptor_upsert
    
    def client(self):
        if self.client is None:
            headers = {
                "x-api-key": self.token
            }
            transport = AIOHTTPTransport(url=self.gqlurl, headers=headers)

            self.client =  Client(
                transport=transport,
                fetch_schema_from_transport=True,
                execute_timeout=30,
            )
        return self.client 
    
    def study_list(self):
        if self.studies is None:
            client = self.client()

            variables = {"first": 10}
            response = client.execute(self.study_query, variable_values=variables)

            self.studies = {}
            for node in response["viewer"]["organizationUsers"]["edges"]:
                org = node["node"]["organization"]
                org_name = org["name"]
                org_id = org["id"]

                self.studies[org_name] = {
                    "description": org["description"],
                    "email": org["email"],
                    "id": org["id"],
                    "name": org_name,
                    "studies": {},
                }

                for study in org["studies"]["edges"]:
                    node = study["node"]
                    self.studies[org_name]["studies"][node["name"]] = {
                        "id": node["id"],
                        "name": node["name"],
                        "globalId": node["globalId"],
                    }

        return self.studies 
    
    def list_descriptors(self, study_global_id):
        headers = {"x-api-key": self.token, "accept": "application/json"}
        rest_url = f"{self.dwrest}/studies/{study_global_id}/global-descriptors?descriptors=all"
        response = requests.get(rest_url, headers=headers, timeout=None)
        return response.json()
    
    def update_descriptors(self, study_global_id, descriptors):
        """
        Uploads the descriptors to the rest API and then gets the updated version via a graphQL call


        return:
          dict: descriptor => global ID for all descriptors provided
        """

        headers = {
            "x-api-key": self.token,
            "accept": "application/json",
            "content-type": "application/json",
        }
        # pdb.set_trace()
        resp = requests.post(
            f"https://dewrangle.com/api/rest/studies/{study_global_id}/files/descriptors.json",
            headers=headers,
            json=descriptors,
            timeout=None,
        )
        fileid = resp.json()["id"]
        variables = {"input": {"studyFileId": fileid, "skipUnavailableDescriptors": True}}
        response = self.client.execute(descriptor_upsert, variable_values=variables)

        job_output = response["globalDescriptorUpsert"]["job"]
        ids = {}

        # pdb.set_trace()
        for item in job_output["globalDescriptors"]["edges"]:
            item_data = item["node"]

        return ids