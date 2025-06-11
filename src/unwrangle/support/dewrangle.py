# Support for dewrangle queries


from graphql import print_ast
import sys
import time 
from gql.transport.aiohttp import AIOHTTPTransport
from gql import Client, gql
import requests
from . import _support_details
import json

import pdb

from rich import print

JOB_POLLING_COUNT = 20
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
        self._study_by_org_query = None
        self._organization_query = None 
        self._descriptor_upsert = None
        self._query_jobdescriptorupsert = None 

        self.organizations = None

        self.studies = None 

    @property
    def study_query(self):
        if self._study_query is None:
            self._study_query = gql((_support_details / "query_studies.gql").read_text())
        return self._study_query

    @property 
    def study_by_org_query(self):
        if self._study_by_org_query is None:
            self._study_by_org_query = gql((_support_details / "query_studies_by_org.gql").read_text())
        return self._study_by_org_query 

    @property
    def organization_query(self):
        if self._organization_query is None:
            self._organization_query = gql((_support_details / "query_orgaizations.gql").read_text())
        return self._organization_query

    @property
    def descriptor_upsert(self):
        if self._descriptor_upsert is None:
            self._descriptor_upsert = gql((_support_details / "descriptor_upsert_mutation.gql").read_text())
        return self._descriptor_upsert

    @property 
    def query_jobdescriptorupsert(self):
        if self._query_jobdescriptorupsert is None:
            self._query_jobdescriptorupsert = gql((_support_details/"query_jobdescriptorupsert.gql").read_text())
        return self._query_jobdescriptorupsert
    
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
    
    def organization_list(self, pagesize=10):
        if self.organizations is None:
            client = self.client()
            variables = {"first": pagesize}
            self.organizations = {}

            has_next_page = True 

            while has_next_page:
                response = client.execute(self.study_query, variable_values=variables)
                for node in response["viewer"]["organizationUsers"]["edges"]:
                    org = node["node"]["organization"]

                    self.organizations[org['name']] = {
                        "name": org['name'],
                        "description": org['description'],
                        "email": org['email'],
                        "id": org['id']
                    }

                
                page_info = response["viewer"]["organizationUsers"].get('pageInfo')
                if page_info is not None:
                    has_next_page = page_info['has_next_page']

                    if has_next_page:
                        variables.update({"after": page_info['endCursor']})
                else:
                    has_next_page = False

        return self.organizations

    def study_list(self, organization_name = None, pagesize=10):
        if self.studies is None:
            client = self.client()

            self.studies = {}
            for org_name, org in self.organization_list().items():
                org_id = org['id']

                variables = {"first": pagesize, "id": org_id}
                has_next_page = True 

                self.studies[org_name] = {
                    "description": org["description"],
                    "email": org["email"],
                    "id": org["id"],
                    "name": org_name,
                    "studies": {},
                }
                while has_next_page:
                    response = client.execute(self.study_by_org_query, variable_values=variables)
                    studies_node = response["node"]["studies"]


                    for study in studies_node['edges']:
                        node = study["node"]
                        self.studies[org_name]["studies"][node["name"]] = {
                            "id": node["id"],
                            "name": node["name"],
                            "globalId": node["globalId"],
                        }

                    pinfo = studies_node['pageInfo']
                    has_next_page = pinfo['hasNextPage']
                    variables.update({"after": pinfo['endCursor']})

        if organization_name is not None:
            return [self.studies[organization_name]]
        return self.studies 
    
    def study_details(self, study_name, org_name=None):
        studies = self.study_list()
        if org_name is None:
            if len(studies) > 1:
                print(studies)
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
        # pdb.set_trace()
        ids = {}

        if len(descriptors) > 0:
            with open("output/job_descriptors.json", 'wt', encoding='utf-8') as f:
                json.dump(descriptors, f, ensure_ascii=False, indent=2)

            headers = {
                "x-api-key": self.token,
                "accept": "application/json",
                "content-type": "application/json",
            }
            print(f"- Minting {len(descriptors)} ids with dewrangle.")
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
            
            response = self.client().execute(self.descriptor_upsert, variable_values=variables)

            if response["globalDescriptorUpsert"].get("errors") is not None:
                print(response["globalDescriptorUpsert"].get("errors"))
                pdb.set_trace()


            if "job" in response["globalDescriptorUpsert"] and response["globalDescriptorUpsert"]["job"] is not None:
                job_output = response["globalDescriptorUpsert"]["job"]
                job_id = job_output['id']

            # With the job ID, we will now query JobGlobalDescriptorUpsert to determine:
            # 1) is the job done?
            # 2) Where there any errors?


            # We'll try this for a while before giving up
            tries = JOB_POLLING_COUNT
            total_time_slept = 0
            while tries > 0:
                tries -= 1
                response = self.client().execute(self.query_jobdescriptorupsert, variable_values={"id": job_id})

                if response['node']['completedAt'] is None:
                    delay = (JOB_POLLING_COUNT-tries) * (JOB_POLLING_COUNT-tries)
                    print(f"- Job: {study_id}:{job_id} not completed. Waiting {delay}s before trying again. {total_time_slept}s previously waited. ")     

                    total_time_slept += delay
                    time.sleep(delay)               
                    
                else:
                    tries = 0

                    response_content = response['node']
                    if response_content['errorAggregation'] is not None and response_content['errorAggregation']['totalErrorCount'] > 0:
                        print(f"{response_content['errorAggregation']['totalErrorCount']} error(s) found: ")
                        for node in response_content['errors']['edges']:
                            print(f"- [bold]{node['node']['name']}[/bold] - [red]{node['node']['message']}[/red]")
                        
                        sys.exit(1)
                    print(f"Job {job_id} completed. ")

            resp = requests.get(f"https://dewrangle.com/api/rest/studies/{study_id}/global-descriptors?job={job_id}", headers=headers, timeout=None)

            local_ids = {}
            if not resp:
                print(resp)
                print(resp.json())
                print("There was a problem with the REST query for results:")
                pdb.set_trace()
            for item in resp.json():
                local_ids[item['descriptor']] = item 


            if len(descriptors) != len(local_ids):
                print(descriptors)
                print("----")
                print(response)
                print("----")
                sys.stderr.write(f"- Weird. Job {study_id}:{job_id} returned {len(ids)} out of {len(descriptors)}.") 
                pdb.set_trace()
            ids = local_ids 

        else:
            print(f"Refusing to mutate a zero length descriptor list. ")

        return ids