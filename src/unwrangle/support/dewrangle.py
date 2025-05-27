# Support for dewrangle queries


from graphql import print_ast
import sys
from gql.transport.aiohttp import AIOHTTPTransport
from gql import Client, gql
import requests
from . import _support_details, descriptor_upsert

class Dewrangler:
    def __init__(self, token, dwrest="https://dewrangle.com/api/rest", gqurl="https://dewrangle.com/api/graphql" ):
        self._client = None

        assert token is not None, "Dewrangle must be given a valid dewrangle token!"
        # Token associated with the dewrangle client access
        self.token = token
        self.gqurl = gqurl          # Dewrangle GraphQL URL
        self.dwrest = dwrest        # REST Api URL 

        # GraphQL query associated with gathering studies we can access
        self._study_query = None
        self._descriptor_upsert = None

        self.studies = None 

    @property
    def study_query(self):
        if self._study_query is None:
            self._study_query = gql((_support_details / "query_studies.gql").read_text())
        return self._study_query

    @property
    def descriptor_upsert(self):
        if self._descriptor_upsert is None:
            self._descriptor_upsert = gql((_support_details / "descriptor_upsert.gql").read_text())
        return self._descriptor_upsert
    
    def client(self):
        if self._client is None:
            headers = {
                "x-api-key": self.token
            }
            transport = AIOHTTPTransport(url=self.gqurl, headers=headers)

            self._client =  Client(
                transport=transport,
                fetch_schema_from_transport=True,
                execute_timeout=30,
            )
        return self._client 
    
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
    
    def study_details(self, study_name, org_name=None):
        studies = self.study_list()
        if org_name is None:
            if len(studies) > 1:
                org_list = ', '.join(studies)
                sys.stderr.write(f"More than one organization found ({org_list}). Please specify which organization the study belongs to. \n")
                sys.exit(1)
            else:
                org_name = list(self.studies.keys())[0]
        
        if study_name not in studies[org_name]['studies']:
            sys.stderr.write(f"No matching study found for '{study_name}' in organization, '{org_name}'\n")
            study_list = "\n\t* ".join(studies[org_name]['studies'].keys())
            sys.stderr.write(f"Available study names include: \n\t* {study_list}\n")
            sys.exit(1)

        return studies[org_name]['studies'][study_name]

        
    
    def list_descriptors(self, study_id):
        headers = {"x-api-key": self.token, "accept": "application/json"}
        rest_url = f"{self.dwrest}/studies/{study_id}/global-descriptors?descriptors=all"
        response = requests.get(rest_url, headers=headers, timeout=None)
        return response.json()
    
    def update_descriptors(self, study_id, descriptors):
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
            f"https://dewrangle.com/api/rest/studies/{study_id}/files/descriptors.json",
            headers=headers,
            json=descriptors,
            timeout=None,
        )
        fileid = resp.json()["id"]

        variables = {"input": {"studyId": study_id, 
                       "studyFileIds": [{ "id": fileid}], "skipUnavailableDescriptors": True}}
        
        response = self.client.execute(descriptor_upsert, variable_values=variables)

        job_output = response["globalDescriptorUpsert"]["job"]
        job_id = job_output['id']

        resp = requests.get(f"https://dewrangle.com/api/rest/studies/{study_id}/global-descriptors?job={job_id}", headers=headers, timeout=None)
        
        ids = {}

        for item in resp.json():
            ids[item['descriptor']] = item 

        return ids