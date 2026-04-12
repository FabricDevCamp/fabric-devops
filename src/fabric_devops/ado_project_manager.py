"""Module to manage calls to Fabric REST APIs"""
import base64
import json
import datetime
import os
import time
from json.decoder import JSONDecodeError

import requests

import msal

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings
from .item_definition_factory import ItemDefinitionFactory

class AdoProjectManager:
    """Wrapper class for calling Azure REST APIs for Azure Dev Ops"""

    AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')
    AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
    AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')

    ADO_ORGANIZATION = os.getenv('ADO_ORGANIZATION')
    BASE_URL = f'https://dev.azure.com/{ADO_ORGANIZATION}/_apis/'

    ADO_PROJECT_TEMPLATE_ID = "b8a3a935-7e91-48b8-a94c-606d37c3e9f2"
    ADO_API_VERSION = "api-version=7.1-preview"

    #region Low-level details about authentication and HTTP requests and responses

    # in-memory token cache
    _token_cache = dict()

    @classmethod
    def _get_ado_access_token(cls):
        """"Get Access Token for Azure Dev """
        ado_resource_id = '499b84ac-1321-427f-aa17-267ca6975798'
        scope = ado_resource_id + "/.default"
        
        if (scope in cls._token_cache) and \
           (datetime.datetime.now() < cls._token_cache[scope]['access_token_expiration']):
            return cls._token_cache[scope]['access_token']
      
        app = msal.ConfidentialClientApplication(
            cls.AZURE_CLIENT_ID,
            authority=f'https://login.microsoftonline.com/{cls.AZURE_TENANT_ID}',
            client_credential=cls.AZURE_CLIENT_SECRET)

        authentication_result = app.acquire_token_for_client([scope])

        cls._token_cache[scope] = {
            'access_token': authentication_result['access_token'],
            'access_token_expiration': datetime.datetime.now() + \
                                       datetime.timedelta(0,  int(authentication_result['expires_in']))
        }

        return authentication_result['access_token']

    @classmethod
    def _execute_get_request(cls, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = cls.BASE_URL + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json: {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_get_request_on_vssps(cls, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = f'https://vssps.dev.azure.com/{cls.ADO_ORGANIZATION}/_apis/' + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json: {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_get_request_on_project(cls, project_name, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = f'https://dev.azure.com/{cls.ADO_ORGANIZATION}/{project_name}/_apis/' + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json: {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request(cls, endpoint, post_body=''):
        """Execute POST request with support for Long-running Operations (LRO)"""
        rest_url = cls.BASE_URL + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json; charset=utf-8; {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}

        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code in { 202 }:

            reponse_body = response.json()
            operation_id = reponse_body['id']
            operation_url = f'operations/{operation_id}'

            time.sleep(2)
            response = cls._execute_get_request(operation_url)
            
            while response['status'] not in [ 'succeeded', 'failed', 'cancelled' ]:
                time.sleep(2)
                response = cls._execute_get_request(operation_url)

            if response['status'] == 'succeeded':
                return None
            else:
                AppLogger.log_error(response)

        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request_on_project(cls, project_name, endpoint, post_body=''):
        """Execute POST request with support for Long-running Operations (LRO)"""
        rest_url = f'https://dev.azure.com/{cls.ADO_ORGANIZATION}/{project_name}/_apis/' + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json; charset=utf-8; {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}

        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code in { 202 }:

            reponse_body = response.json()
            operation_id = reponse_body['id']
            operation_url = f'operations/{operation_id}'

            time.sleep(2)
            response = cls._execute_get_request(operation_url)
            
            while response['status'] not in [ 'succeeded', 'failed', 'cancelled' ]:
                time.sleep(2)
                response = cls._execute_get_request(operation_url)

            if response['status'] == 'succeeded':
                return None
            else:
                AppLogger.log_error(response)

        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_patch_request(cls, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = cls.BASE_URL + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json; {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.patch(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()

        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_patch_request(endpoint, post_body)
        else:
            AppLogger.log_error(
                f'Error executing PATCH request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_patch_request_on_project(cls, project_name, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = f'https://dev.azure.com/{cls.ADO_ORGANIZATION}/{project_name}/_apis/' + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json; {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}
        
        response = requests.patch(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()

        else:
            AppLogger.log_error(
                f'Error executing PATCH request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_put_request_on_project(cls, project_name, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = f'https://dev.azure.com/{cls.ADO_ORGANIZATION}/{project_name}/_apis/' + endpoint
        access_token = cls._get_ado_access_token()
        request_headers = {'Accept': f'application/json; {cls.ADO_API_VERSION}',
                           'Content-Type': f'application/json; {cls.ADO_API_VERSION}',
                           'Authorization': f'Bearer {access_token}'}
        
        response = requests.put(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()

        else:
            AppLogger.log_error(
                f'Error executing PUT request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_delete_request(cls, endpoint):
        """Execute DELETE Request on Fabric REST API Endpoint"""
        rest_url = cls.BASE_URL + endpoint
        access_token = cls._get_ado_access_token()
        request_headers= {'Accept':  f'application/json; {cls.ADO_API_VERSION}',
                          'Content-Type':  f'application/json; {cls.ADO_API_VERSION}',
                          'Authorization': f'Bearer {access_token}'}
        response = requests.delete(url=rest_url, headers=request_headers, timeout=60)
        
        if response.status_code in { 200, 204}:
            return None

        if response.status_code in { 202 }:

            reponse_body = response.json()
            operation_id = reponse_body['id']
            operation_url = f'operations/{operation_id}'

            time.sleep(2)
            response = cls._execute_get_request(operation_url)
            
            while response['status'] not in [ 'succeeded', 'failed', 'cancelled' ]:
                time.sleep(2)
                response = cls._execute_get_request(operation_url)

            if response['status'] == 'succeeded':
                return None
            else:
                AppLogger.log_error(response)

        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            cls._execute_delete_request(endpoint)
            return None
        else:
            AppLogger.log_error(
                f'Error executing DELETE request: {response.status_code} - {response.text}')
            return None

#endregion

    @classmethod
    def get_projects(cls):
        """Get Azure DevOps Projects"""
        endpoint = "projects"
        return cls._execute_get_request(endpoint)['value']
    
    @classmethod
    def get_project(cls, project_name):
        """Get Project"""
        projects = cls.get_projects()
        for project in projects:
            if project['name'] == project_name:
                return project
        return None

    @classmethod
    def create_project(cls, project_name, workspace = None):
        """Create Project"""
        AppLogger.log_step(f'Creating new ADO project [{project_name}]')
        existing_projects = cls.get_projects()
        for existing_project in existing_projects:
            if existing_project['name'] == project_name:
                AppLogger.log_substep('Deleteting existing project with same name')
                cls.delete_project(existing_project['id'])
                break

        AppLogger.log_substep("Calling Create Project API")
        
        workspace_description = 'This is a Azure DevOps project created to demonstrate GIT integration with Fabric workspace.' 
        
        if workspace is not None:
            workspace_description += \
                f' This repository is connected to a Fabric workspace named [{workspace.display_name}] with an id of [{workspace.id}]'            
        
        endpoint = "projects"
        body = {
            "name": project_name,
            "description": workspace_description,
            "state": "unchanged",
            "capabilities": {
                "versioncontrol": { "sourceControlType": "Git" },
                "processTemplate": { "templateTypeId": "b8a3a935-7e91-48b8-a94c-606d37c3e9f2" }
            },
            "visibility": "private"
        }
        
        cls._execute_post_request(endpoint, body)
        time.sleep(3)
        new_project = cls.get_project(project_name)         
        while (new_project is None) or (new_project['state'] != 'wellFormed'):
            time.sleep(3)
            new_project = cls.get_project(project_name)
            
        AppLogger.log_substep(f"New ADO project created with Id [{new_project['id']}]")

        repository = cls.get_project_repository(project_name)
        
        if workspace is None:
            root_folder_readme_content = ItemDefinitionFactory.get_template_file(
                'AdoProjectTemplates/AdoReadMe.md')
        else:
            root_folder_readme_content = ItemDefinitionFactory.get_template_file(
                'AdoProjectTemplates/AdoReadMeWithWorkspace.md')
            
            root_folder_readme_content = root_folder_readme_content.replace(
                '{WORKSPACE_ID}', 
                workspace.id)

            root_folder_readme_content = root_folder_readme_content.replace(
                '{WORKSPACE_NAME}', 
                workspace.display_name)

        push_endpoint = f"git/repositories/{repository['id']}/pushes"
        push_body = {
            "commits":[
                {
                    "comment":"Adding ReadMe.md to root folder of repository",
                    "changes":[
                        {
                            "item":{ "path":"/ReadMe.md"},
                            "changeType":"add",
                            "newContent":{
                                "content": root_folder_readme_content
                            }
                         }
                        ]
                    }],
                    "refUpdates":[
                        {
                            "name":"refs/heads/main",
                            "oldObjectId":"0000000000000000000000000000000000000000"
                        }
                    ]
                }
        cls._execute_post_request(push_endpoint, push_body)

        workspace_folder_readme_content = ItemDefinitionFactory.get_template_file(
            'AdoProjectTemplates/AdoReadMeForWorkspaceFolder.md')

        cls.write_file_to_repo(
            project_name, 
            'main', 
            'workspace/ReadMe.md', 
            workspace_folder_readme_content,
            comment='Adding ReadMe.md to [workspace] folder')
        
        return new_project
      
    @classmethod
    def delete_project(cls, project_id):
        """Delete Project"""
        endpoint = f'projects/{project_id}'
        return cls._execute_delete_request(endpoint)   
    
    @classmethod
    def delete_project_by_name(cls, project_name):
        """Delete Project By Name"""
        project = cls.get_project(project_name)
        cls.delete_project(project['id'])
    
    @classmethod
    def delete_all_projects(cls):
        """Delete All Projects"""
        projects = cls.get_projects()
        for project in projects:
            cls.delete_project(project['id'])    
    
    @classmethod
    def get_project_repositories(cls, project_name):
        """Get Project Repositories"""
        endpoint = 'git/repositories'
        return cls._execute_get_request_on_project(project_name, endpoint)['value']

    @classmethod
    def get_project_repository(cls, project_name):
        """Get Project Repository"""
        repo_list = cls.get_project_repositories(project_name)
        return repo_list[0]
        
    @classmethod
    def get_branches(cls, project_name):
        """Get Branches"""
        repository = cls.get_project_repository(project_name)
        repository_id = repository['id']
        endpoint = f'git/repositories/{repository_id}/refs'
        return cls._execute_get_request_on_project(project_name, endpoint)['value']

    @classmethod
    def get_branch(cls, project_name, branch):
        """Get Branches"""
        repository = cls.get_project_repository(project_name)
        repository_id = repository['id']
        endpoint = f'git/repositories/{repository_id}/refs/?filter=heads/{branch}'
        return cls._execute_get_request_on_project(project_name, endpoint)['value'][0]

    @classmethod
    def create_branch(cls, project_name, branch, base_branch):
        """Get Branches"""
        AppLogger.log_substep(f'Creating branch [{branch}]')
        repository = cls.get_project_repository(project_name)        
        repository_id = repository['id']
        base_branch = cls.get_branch(project_name, base_branch)
        base_branch_object_id = base_branch['objectId']
        refs_endpoint = f'git/repositories/{repository_id}/refs'
        body = [{
            "name": f'refs/heads/{branch}',
            "oldObjectId":"0000000000000000000000000000000000000000",
            "newObjectId":f'{base_branch_object_id}'
        }]
        return cls._execute_post_request(refs_endpoint, body)

    @classmethod
    def set_default_branch(cls, project_name, branch):
        """Get Branches"""
        AppLogger.log_substep(f'Setting [{branch}] as default branch')
        repository = cls.get_project_repository(project_name)
        repository_id = repository['id']
        repo_endpoint = f'git/repositories/{repository_id}/'
        body = { "defaultBranch": f"refs/heads/{branch}" }
        return cls._execute_patch_request_on_project(project_name, repo_endpoint, body)

    @classmethod
    def add_standard_branches(cls, project_name):
        """Add Standard Branches"""     
        cls.create_branch(project_name, 'test', 'main')
        cls.create_branch(project_name, 'dev', 'test')
        cls.set_default_branch(project_name, "dev")

    @classmethod
    def copy_project_files_to_ado_repo(
        cls,
        project_name,
        branch,
        folder_path,
        comment = 'Adding files from template folder',
        variable_group_id = None):       
        """Copy files to ADO repo with automaitc pipeline registration"""        
        AppLogger.log_substep(f"Copy [{folder_path}] files to [{project_name}] repository") 
        
        repository = cls.get_project_repository(project_name)
        target_branch = cls.get_branch(project_name, branch)
        old_object_id = target_branch['objectId']
        
        file_changes = []
        pipelines = []
        folder_path = f".//templates//ProjectWorkflowFiles//{folder_path}//"
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                file = open(file_path,'r', encoding="utf-8")
                file_content = file.read()
                base64_content = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')
                relative_file_path = file_path.replace(folder_path, '').replace('\\', '/')
                
                file_changes.append({
                    "item":{ "path": relative_file_path },
                    "changeType":"add",
                    "newContent": {
                        "content": f"{base64_content}",
                        "contentType": "base64encoded"
                    }
                })
                                
                if relative_file_path.lower().endswith('.yml') and '.pipelines/' in relative_file_path:
                    pipeline_name = relative_file_path.replace('.yml', '').replace('.pipelines/', '')
                    pipelines.append({
                        'pipeline_name': pipeline_name,
                        'relative_file_path': relative_file_path
                })

        push_endpoint = f"git/repositories/{repository['id']}/pushes"
        push_body = {
            "refUpdates": [{
                "name": f"refs/heads/{branch}",
                "oldObjectId": f"{old_object_id}"
            }],
            "commits": [{
                "comment": comment,
                "changes": file_changes 
            }]
        }

        cls._execute_post_request(push_endpoint, push_body)
        
        # register pipelines for any YAML files that were copied to .pipelines folder 
        for pipeline in pipelines:
            cls.create_pipeline(
                project_name,
                pipeline['pipeline_name'],
                pipeline['relative_file_path'],
                variable_group_id
            )

    @classmethod
    def write_file_to_repo(cls, project_name, branch, file_path, file_content, comment = None):
        """Write Files to Repo"""        
        
        if comment is None:
            comment = f'Adding file [{file_path}]'
        
        base64_content = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')
        
        repository = cls.get_project_repository(project_name)
        target_branch = cls.get_branch(project_name, branch)
        old_object_id = target_branch['objectId']

        push_endpoint = f"git/repositories/{repository['id']}/pushes"
        push_body = {
            "commits":[
                {
                    "comment": comment,
                    "changes":[
                        {
                            "item":{ "path": file_path },
                            "changeType":"add",
                            "newContent":{
                                "content": F"{base64_content}",
                                "contentType": "base64encoded"
                            }
                         }
                        ]
                    }],
                    "refUpdates": [
                        {
                            "name": f"refs/heads/{branch}",
                            "oldObjectId": f"{old_object_id}"
                        }
                    ]
                }
        
        cls._execute_post_request(push_endpoint, push_body)

    @classmethod
    def write_files_to_repo_from_folder(cls, project_name, branch, folder_path, comment = 'Adding files from template folder'):
        """Write Files to repo from local folder"""        
        AppLogger.log_substep(f"Copy [{folder_path}] files to [{project_name}] repository") 
        
        repository = cls.get_project_repository(project_name)
        target_branch = cls.get_branch(project_name, branch)
        old_object_id = target_branch['objectId']

        file_changes = []
        pipelines = []
        folder_path = f".//templates//ProjectWorkflowFiles//{folder_path}//"
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                file = open(file_path,'r', encoding="utf-8")
                file_content = file.read()
                base64_content = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')
                relative_file_path = file_path.replace(folder_path, '').replace('\\', '/')
                
                file_changes.append({
                    "item":{ "path": relative_file_path },
                    "changeType":"add",
                    "newContent": {
                        "content": f"{base64_content}",
                        "contentType": "base64encoded"
                    }
                })
                
                if relative_file_path.lower().endswith('.yml') and '.pipelines/' in relative_file_path:
                    pipeline_name = relative_file_path.replace('.yml', '').replace('.pipelines/', '')
                    pipelines.append({
                        'pipeline_name': pipeline_name,
                        'relative_file_path': relative_file_path
                    })


        push_endpoint = f"git/repositories/{repository['id']}/pushes"
        push_body = {
            "refUpdates": [{
                "name": f"refs/heads/{branch}",
                "oldObjectId": f"{old_object_id}"
            }],
            "commits": [{
                "comment": comment,
                "changes": file_changes 
            }]
        }

        cls._execute_post_request(push_endpoint, push_body)
        
        for pipeline in pipelines:
            cls.create_pipeline(
                project_name, 
                pipeline['pipeline_name'], 
                pipeline['relative_file_path'], 
                variable_group_id
        )

    @classmethod
    def copy_files_from_folder_to_repo(cls, project_name, branch, folder_path, variable_group_id):
        """Copy files to repo and create pipeline when copying YAML files"""                    

        folder_path = f".//templates//ProjectWorkflowFiles//{folder_path}//"
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                file = open(file_path,'r', encoding="utf-8")
                file_content = file.read()

                relative_file_path = file_path.replace(folder_path, '').replace('\\', '/')
                                                              
                cls.write_file_to_repo(project_name, branch, relative_file_path, file_content)
                
                is_yml_pipeline_file = relative_file_path.lower().endswith('.yml') and '.pipelines/' in relative_file_path
                
                if is_yml_pipeline_file:
                    pipeline_name = relative_file_path.replace('.yml', '').replace('.pipelines/', '')
                    cls.create_pipeline(project_name, pipeline_name, relative_file_path, variable_group_id)

    @classmethod
    def create_one_stage_variable_group(cls, name, project_name, prod_workspace_id):
        """Add Variable Group"""
        AppLogger.log_step(f"Creating variable group [{name}]")
        project = cls.get_project(project_name)
        endpoint = 'distributedtask/variablegroups'
        body = {
            "name": name,
            "description": "Variable group added through code",
            "type": "Vsts",
            "variables": {
                "AZURE_CLIENT_ID": {
                    "value": EnvironmentSettings.AZURE_CLIENT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "AZURE_CLIENT_SECRET": {
                    "value": EnvironmentSettings.AZURE_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "AZURE_TENANT_ID": {
                    "value": EnvironmentSettings.AZURE_TENANT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "FABRIC_CAPACITY_ID": {
                    "value": EnvironmentSettings.FABRIC_CAPACITY_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "ADMIN_USER_ID": {
                    "value": EnvironmentSettings.ADMIN_USER_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "DEVELOPERS_GROUP_ID": {
                    "value": EnvironmentSettings.DEVELOPERS_GROUP_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "WORKSPACE_ID_PROD": {
                    "value": prod_workspace_id,
                    "isSecret": False,
                    "isReadOnly": True
                }                    
            },
            "variableGroupProjectReferences": [{
                "name": name,
                "description": "Variable group added through code",
                "projectReference": {
                    "id": project['id'],
                    "name": project_name
                }
            }]
        }

        return cls._execute_post_request(endpoint, body)

    @classmethod
    def create_two_stage_variable_group(cls, name, project_name, dev_workspace_id, prod_workspace_id):
        """Add Variable Group"""
        AppLogger.log_step(f"Creating variable group [{name}]")
        project = cls.get_project(project_name)
        endpoint = 'distributedtask/variablegroups'
        body = {
            "name": name,
            "description": "Variable group added through code",
            "type": "Vsts",
            "variables": {
                "AZURE_CLIENT_ID": {
                    "value": EnvironmentSettings.AZURE_CLIENT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "AZURE_CLIENT_SECRET": {
                    "value": EnvironmentSettings.AZURE_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "AZURE_TENANT_ID": {
                    "value": EnvironmentSettings.AZURE_TENANT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "FABRIC_CAPACITY_ID": {
                    "value": EnvironmentSettings.FABRIC_CAPACITY_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "ADMIN_USER_ID": {
                    "value": EnvironmentSettings.ADMIN_USER_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "DEVELOPERS_GROUP_ID": {
                    "value": EnvironmentSettings.DEVELOPERS_GROUP_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "WORKSPACE_ID_DEV": {
                    "value": dev_workspace_id,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "WORKSPACE_ID_PROD": {
                    "value": prod_workspace_id,
                    "isSecret": False,
                    "isReadOnly": True
                }                    
            },
            "variableGroupProjectReferences": [{
                "name": name,
                "description": "Variable group added through code",
                "projectReference": {
                    "id": project['id'],
                    "name": project_name
                }
            }]
        }

        return cls._execute_post_request(endpoint, body)

    @classmethod
    def create_variable_group_for_ado_project(
        cls, 
        name, 
        project_name, 
        dev_workspace_id, 
        test_workspace_id,
        prod_workspace_id):
        """Add Variable Group"""
        AppLogger.log_step(f"Creating variable group [{name}]")
        project = cls.get_project(project_name)
        endpoint = 'distributedtask/variablegroups'
        body = {
            "name": name,
            "description": "Variable group added through code",
            "type": "Vsts",
            "variables": {
                "AZURE_CLIENT_ID": {
                    "value": EnvironmentSettings.AZURE_CLIENT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "AZURE_CLIENT_SECRET": {
                    "value": EnvironmentSettings.AZURE_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "AZURE_TENANT_ID": {
                    "value": EnvironmentSettings.AZURE_TENANT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "FABRIC_CAPACITY_ID": {
                    "value": EnvironmentSettings.FABRIC_CAPACITY_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "ADMIN_USER_ID": {
                    "value": EnvironmentSettings.ADMIN_USER_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "DEVELOPERS_GROUP_ID": {
                    "value": EnvironmentSettings.DEVELOPERS_GROUP_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "AZURE_STORAGE_SAS_TOKEN" : {
                    "value": EnvironmentSettings.AZURE_STORAGE_SAS_TOKEN,
                    "isSecret": True,
                    "isReadOnly": True  
                },
                "WORKSPACE_ID_DEV": {
                    "value": dev_workspace_id,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "WORKSPACE_ID_TEST": {
                    "value": test_workspace_id,
                    "isSecret": False,
                    "isReadOnly": True
                },                
                "WORKSPACE_ID_PROD": {
                    "value": prod_workspace_id,
                    "isSecret": False,
                    "isReadOnly": True
                }                    
            },
            "variableGroupProjectReferences": [{
                "name": name,
                "description": "Variable group added through code",
                "projectReference": {
                    "id": project['id'],
                    "name": project_name
                }
            }]
        }

        return cls._execute_post_request(endpoint, body)

    @classmethod
    def create_variable_group(cls, name, project_name):
        """Add Variable Group"""
        AppLogger.log_step(f"Creating variable group [{name}]")
        project = cls.get_project(project_name)
        endpoint = 'distributedtask/variablegroups'
        body = {
            "name": name,
            "description": "Variable group added through code",
            "type": "Vsts",
            "variables": {
                "AZURE_CLIENT_ID": {
                    "value": EnvironmentSettings.AZURE_CLIENT_ID,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "AZURE_CLIENT_SECRET": {
                    "value": EnvironmentSettings.AZURE_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "AZURE_TENANT_ID": {
                    "value": EnvironmentSettings.AZURE_TENANT_ID,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "FABRIC_CAPACITY_ID": {
                    "value": EnvironmentSettings.FABRIC_CAPACITY_ID,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "ADMIN_USER_ID": {
                    "value": EnvironmentSettings.ADMIN_USER_ID,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "SERVICE_PRINCIPAL_OBJECT_ID": {
                    "value": EnvironmentSettings.SERVICE_PRINCIPAL_OBJECT_ID,
                    "isSecret": True,
                    "isReadOnly": True
                }
            },
            "variableGroupProjectReferences": [{
                "name": name,
                "description": "Variable group added through code",
                "projectReference": {
                    "id": project['id'],
                    "name": project_name
                }
            }]
        }

        return cls._execute_post_request(endpoint, body)

    @classmethod
    def list_pipelines(cls, project_name):
        """List Pipelines"""
        return cls._execute_get_request_on_project(project_name, 'pipelines')['value']
   
    @classmethod
    def get_pipeline_by_name(cls, project_name, pipeline_name):
        """Get Pipeline By Name"""
        pipelines = cls.list_pipelines(project_name)
        for pipeline in pipelines:
            if pipeline['name'] == pipeline_name:
                return pipeline

    @classmethod
    def get_pipeline_id_by_name(cls, project_name, pipeline_name):
        """Get Pipeline By Name"""
        pipelines = cls.list_pipelines(project_name)
        for pipeline in pipelines:
            if pipeline['name'] == pipeline_name:
                return pipeline['id']

    @classmethod
    def create_pipeline(cls, project_name, pipeline_name, pipeline_yml_file, variable_group_id = None):
        """Create Pipeline"""
        AppLogger.log_step(f"Creating pipeline [{pipeline_name}]")
        repository = cls.get_project_repository(project_name)
        endpoint = 'pipelines'
        body = {
            "name": pipeline_name,
            "folder":"",
            "configuration":{
                "type":"yaml",
                "path": pipeline_yml_file,
                "repository":{
                    "name": project_name,
                    'id': repository['id'],
                    "type":"azureReposGit"
                }
            }
        }
        
        pipeline = cls._execute_post_request_on_project(project_name, endpoint, body)
        pipeline_id = pipeline['id']
        if variable_group_id is not None:
            cls.set_pipeline_permission(project_name, variable_group_id, pipeline_id)

    @classmethod
    def list_pipeline_runs(cls, project_name, pipeline_id):
        """Get pipeline runs"""
        endpoint = f'pipelines/{pipeline_id}/runs/'
        return cls._execute_get_request_on_project(project_name, endpoint)

    @classmethod
    def get_pipeline_run(cls, project_name, pipeline_id, run_id):
        """Get pipeline run"""
        endpoint = f'pipelines/{pipeline_id}/runs/{run_id}'
        return cls._execute_get_request_on_project(project_name, endpoint)

    @classmethod
    def get_pipeline_run_state(cls, project_name, pipeline_id, run_id):
        """Get pipeline run state"""
        endpoint = f'pipelines/{pipeline_id}/runs/{run_id}'
        return cls._execute_get_request_on_project(project_name, endpoint)['state']

    @classmethod
    def run_pipeline(cls, project_name, pipeline_name, branch_name):
        """Run  Pipeline By Name"""
        AppLogger.log_step(f"Running pipeline [{pipeline_name}] on branch [{branch_name}]")
        pipeline_id = cls.get_pipeline_id_by_name(project_name, pipeline_name)
        endpoint = f'pipelines/{pipeline_id}/runs'
        post_body = {
            "stagesToSkip": [],
            "resources": {
                "repositories": {
                    "self" : { 
                        "refName" : f"refs/heads/{branch_name}"
                    }
                }
            },
            "variables":{}
        }
          
        pipeline_run = cls._execute_post_request_on_project(project_name, endpoint, post_body)
        pipeline_run_id = pipeline_run['id']
        
        pipeline_run_state = cls.get_pipeline_run_state(
            project_name,
            pipeline_id, 
            pipeline_run_id
        )
        
        while pipeline_run_state == 'inProgress':
            time.sleep(20)
            pipeline_run_state = cls.get_pipeline_run_state(
                project_name,
                pipeline_id,
                pipeline_run_id
            )
                
        if pipeline_run_state == 'completed':
            AppLogger.log_substep('Pipeline run completed successfully')
        else:
            AppLogger.log_error(f'Error running pipeline - state={pipeline_run_state}')

    @classmethod
    def set_pipeline_permission(cls, project_name, variable_group_id, pipeline_id):
        """Set pipeline permissions"""
        AppLogger.log_substep("Setting pipeline permissions")
        endpoint = f'pipelines/pipelinePermissions/variablegroup/{variable_group_id}'
        body = {
            "pipelines":[{
                "id":pipeline_id,
                "authorized":True
            }]
        }

        cls._execute_patch_request_on_project(project_name, endpoint, body)

    @classmethod
    def get_pull_requests(cls, project_name):
        """Create Pull Request"""
        repository = cls.get_project_repository(project_name)
        endpoint = f"git/repositories/{repository['id']}/pullrequests"
        return cls._execute_get_request_on_project(project_name, endpoint)

    @classmethod
    def get_pull_request(cls, project_name, pull_request_id):
        """Get Pull Request"""
        repository = cls.get_project_repository(project_name)
        endpoint = f"git/repositories/{repository['id']}/pullrequests/{pull_request_id}"
        return cls._execute_get_request_on_project(project_name, endpoint)        

    @classmethod
    def create_pull_request(cls, project_name, source_branch, 
                            target_branch, title, description = "pull request created with code"):
        """Create Pull Request"""
        repository = cls.get_project_repository(project_name)
        endpoint = f"git/repositories/{repository['id']}/pullrequests"
        body = {
            "sourceRefName": f"refs/heads/{source_branch}" ,            
            "targetRefName": f"refs/heads/{target_branch}" ,
            "title": title,
            "description": description
        }

        pull_request = cls._execute_post_request_on_project(project_name, endpoint, body)
        AppLogger.log_substep(f"Pull request [{title}] with PR Id [{pull_request['pullRequestId']}]")
        return pull_request
    
    @classmethod
    def merge_pull_request(cls, project_name, pull_request, merge_comment = None):
        """Merge Pull Request"""
        AppLogger.log_substep("Approving and completing pull request")
        pull_request_id = pull_request['pullRequestId']
        repository = cls.get_project_repository(project_name)
        endpoint = f"git/repositories/{repository['id']}/pullrequests/{pull_request_id}"
        body = {
            "completionOptions": {
                "bypassPolicy": True,
                "deleteSourceBranch": False,
                "bypassReason": "policy-watcher-automation"
            },
        }
        cls._execute_patch_request_on_project(project_name, endpoint, body)        

   
        reviewer_id = cls.get_service_principal_id()
        
        endpoint = f"git/repositories/{repository['id']}/pullrequests/{pull_request_id}/reviewers/{reviewer_id}"
        body = {
            "id": reviewer_id,
            "vote": 10
        }

        cls._execute_put_request_on_project(project_name, endpoint, body)

        pull_request = cls.get_pull_request(project_name, pull_request_id)
        last_merge_source_commit = pull_request['lastMergeSourceCommit']
        
        endpoint = f"git/repositories/{repository['id']}/pullrequests/{pull_request_id}"
    
        if merge_comment is None:
            merge_comment = f'Merged changes from pull request {pull_request_id}'
        
        body = {
            "status": "completed",
            "autoCompleteSetBy": {
                "id": reviewer_id
            },
            "completionOptions": {
                "mergeStrategy": "NoFastForward",
                "mergeCommitMessage": merge_comment,
                "bypassPolicy": True,
                "deleteSourceBranch": False,
                "bypassReason": "policy-watcher-automation"
            },
            "lastMergeSourceCommit": last_merge_source_commit
        }
        cls._execute_patch_request_on_project(project_name, endpoint, body)        

    @classmethod
    def create_and_merge_pull_request(cls, project_name, source_branch, target_branch, merge_comment = None):
        """Create Pull Request"""
        AppLogger.log_step(f'Creating pull request to push changes from [{source_branch}] to [{target_branch}]')
        title = f'Pushing changes from {source_branch} to {target_branch}'
        pull_request = cls.create_pull_request(project_name, source_branch, target_branch, title, 'Auto commit')
        
        pull_request_id = pull_request['pullRequestId']
        pull_request_status = pull_request['status']
        
        while pull_request_status != 'active':
            AppLogger.log_step(f'Waiting while pull request status is {pull_request_status}')
            time.sleep(3)
            pull_request = cls.get_pull_request(project_name, pull_request_id)
            pull_request_status = pull_request['status']        

        cls.merge_pull_request(project_name, pull_request, merge_comment)

        time.sleep(3)

        pull_request = cls.get_pull_request(project_name, pull_request_id)
        pull_request_status = pull_request['status']

        while pull_request_status != 'completed':
            AppLogger.log_substep('Waiting for pull request to complete')
            time.sleep(3)
            pull_request = cls.get_pull_request(project_name, pull_request_id)
            pull_request_status = pull_request['status']

        AppLogger.log_substep(f'Pull request merge status: {pull_request["mergeStatus"]}')

    @classmethod
    def get_user_id(cls, user_email):
        """Get Azure Dev Ops Identities"""
        endpoint = rf'identities?searchFilter=General&filterValue={user_email}&queryMembership=None' 
        return cls._execute_get_request_on_vssps(endpoint)['value'][0]['id']

    @classmethod
    def get_service_principal_id(cls):
        """Get Azure Dev Ops Identities"""
        endpoint = rf'identities?searchFilter=AccountName&filterValue={EnvironmentSettings.AZURE_TENANT_ID}\{EnvironmentSettings.SERVICE_PRINCIPAL_OBJECT_ID}&queryMembership=None'
        return cls._execute_get_request_on_vssps(endpoint)['value'][0]['id']

    @classmethod
    def create_environment(cls, project_name, environment_name):
        """Create environment in ADO project"""
        AppLogger.log_substep(f"Creating environment [{environment_name}]")
        endpoint = 'distributedtask/environments'
        body = {
            "name": environment_name,
            "description": "Environment created for Fabric CI/CD demo"
        
        }

        return cls._execute_post_request_on_project(project_name, endpoint, body)

    @classmethod
    def add_approval_to_environment(cls, project_name, environment_name, approver_email):
        """Add approval to environment"""                              
        
        AppLogger.log_substep("Adding approval to environment")
        endpoint = 'pipelines/checks/configurations/'
        body = {
            "type": {
                "id": "8c6f20a7-a545-4486-9777-f762fafe0d4d", # Static ID for "Manual Approval"
                "name": "Approval"
            },
            "settings": { 
                "approvers": [ {"id": cls.get_user_id(approver_email) }], # The Identity ID of the approver
                "executionOrder": "anyOrder", # Options: "anyOrder" or "inSequence"
                "minRequiredApprovers": 1,
                "instructions": "Please review this deployment."
            },
            "resource": {
                "type": "environment",
                "id":  cls.get_environment_id(project_name, environment_name) # Numeric ID of the environment            
            }
        }

        return cls._execute_post_request_on_project(project_name, endpoint, body)
       
    @classmethod
    def get_environments(cls, project_name):
        """Get all environments in the project"""
        endpoint = 'distributedtask/environments'
        return cls._execute_get_request_on_project(project_name, endpoint)['value']

    @classmethod
    def get_environment_id(cls, project_name, environment_name):
        """Get the ID of a specific environment in the project"""
        environments = cls.get_environments(project_name)
        for env in environments:
            if env['name'] == environment_name:
                return env['id']
        raise ValueError(f"Environment '{environment_name}' not found")