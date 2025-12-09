"""Module to manage calls to Fabric REST APIs"""
import base64
import os
import time
from json.decoder import JSONDecodeError

import requests

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings
from .entra_id_token_manager import EntraIdTokenManager
from .item_definition_factory import ItemDefinitionFactory

class AdoProjectManager:
    """Wrapper class for calling Azure REST APIs for Azure Dev Ops"""

    ADO_ORGANIZATION = 'FabricDevCamp'
    BASE_URL = f'https://dev.azure.com/{ADO_ORGANIZATION}/_apis/'

    ADO_PROJECT_TEMPLATE_ID = "b8a3a935-7e91-48b8-a94c-606d37c3e9f2";
    ADO_API_VERSION = "api-version=7.1-preview";

#region Low-level details about authentication and HTTP requests and responses

    @classmethod
    def _execute_get_request(cls, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = cls.BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        access_token = EntraIdTokenManager.get_ado_access_token()
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
        access_token = EntraIdTokenManager.get_ado_access_token()
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
                f' This repository is connected to a Fabrc workspace named [{workspace["displayName"]}] with an id of [{workspace["id"]}]'            
        
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
        time.sleep(5)
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
                workspace['id'])

            root_folder_readme_content = root_folder_readme_content.replace(
                '{WORKSPACE_NAME}', 
                workspace['displayName'])

        push_endpoint = f"git/repositories/{repository['id']}/pushes"
        push_body = {
            "commits":[
                {
                    "comment":"Adding ReadMe.md",
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

        cls.write_file_to_repo(project_name, 'main', 'workspace/ReadMe.md', workspace_folder_readme_content)
        
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
    def get_project_repositories(cls, project_name):
        """Get Project Repositories"""
        endpoint = 'git/repositories'
        return cls._execute_get_request_on_project(project_name, endpoint)['value']

    @classmethod
    def get_project_repository(cls, project_name):
        """Get Project Repository"""
        return cls.get_project_repositories(project_name)[0]
    
    @classmethod
    def get_branches(cls, project_name):
        """Get Branches"""
        repository = cls.get_project_repository(project_name)
        repository_id = repository['id']
        endpoint = f'git/repositories/{repository_id}/refs'
        return cls._execute_get_request_on_project(project_name, endpoint)

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
        folder_path = f".//templates//WorkflowActions//{folder_path}//"
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

    @classmethod
    def copy_files_from_folder_to_repo(cls, project_name, branch, folder_path, variable_group_id):
        """Copy files to repo and create pipeline when copying YAML files"""                    

        folder_path = f".//templates//WorkflowActions//{folder_path}//"
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
                "FABRIC_CLIENT_ID": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "FABRIC_CLIENT_SECRET": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "FABRIC_TENANT_ID": {
                    "value": EnvironmentSettings.FABRIC_TENANT_ID,
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
                "FABRIC_CLIENT_ID": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "FABRIC_CLIENT_SECRET": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "FABRIC_TENANT_ID": {
                    "value": EnvironmentSettings.FABRIC_TENANT_ID,
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
    def create_variable_group_for_fabric_cicd(
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
                "FABRIC_CLIENT_ID": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_ID,
                    "isSecret": False,
                    "isReadOnly": True
                },
                "FABRIC_CLIENT_SECRET": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "FABRIC_TENANT_ID": {
                    "value": EnvironmentSettings.FABRIC_TENANT_ID,
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
                "FABRIC_CLIENT_ID": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_ID,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "FABRIC_CLIENT_SECRET": {
                    "value": EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    "isSecret": True,
                    "isReadOnly": True
                },
                "FABRIC_TENANT_ID": {
                    "value": EnvironmentSettings.FABRIC_TENANT_ID,
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
    def create_pull_request(cls, project_name, source_branch, target_branch, title, description = "plain description"):
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
    def merge_pull_request(cls, project_name, pull_request):
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

        reviewer_id = None
        if EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL:
            reviewer_id = cls.get_service_principal_id()
        else:
            reviewer_id = cls.get_user_id('ted@fabricdevcamp.net')

        endpoint = f"git/repositories/{repository['id']}/pullrequests/{pull_request_id}/reviewers/{reviewer_id}"
        body = {
            "id": reviewer_id,
            "vote": 10
        }

        cls._execute_put_request_on_project(project_name, endpoint, body)

        pull_request = cls.get_pull_request(project_name, pull_request_id)
        last_merge_source_commit = pull_request['lastMergeSourceCommit']
        
        endpoint = f"git/repositories/{repository['id']}/pullrequests/{pull_request_id}"
        body = {
            "status": "completed",
            "autoCompleteSetBy": {
                "id": reviewer_id
            },
            "completionOptions": {
                "mergeStrategy": "NoFastForward",
                "mergeCommitMessage": f'Automating merge of PR {pull_request_id}',
                "bypassPolicy": True,
                "deleteSourceBranch": False,
                "bypassReason": "policy-watcher-automation"
            },
            "lastMergeSourceCommit": last_merge_source_commit
        }
        cls._execute_patch_request_on_project(project_name, endpoint, body)        

    @classmethod
    def create_and_merge_pull_request(cls, project_name, source_branch, target_branch):
        """Create Pull Request"""
        AppLogger.log_step(f'Creating pull request to push changes from [{source_branch}] to [{target_branch}]')
        title = f'Pushing changes from {source_branch} to {target_branch}'
        pull_request = cls.create_pull_request(project_name, source_branch, target_branch, title, 'Auto commit')
        time.sleep(5)
        cls.merge_pull_request(project_name, pull_request)

    @classmethod
    def get_user_id(cls, user_email):
        """Get Azure Dev Ops Identities"""
        endpoint = rf'identities?searchFilter=General&filterValue={user_email}&queryMembership=None' 
        return cls._execute_get_request_on_vssps(endpoint)['value'][0]['id']

    @classmethod
    def get_service_principal_id(cls):
        """Get Azure Dev Ops Identities"""
        endpoint = rf'identities?searchFilter=AccountName&filterValue={EnvironmentSettings.FABRIC_TENANT_ID}\{EnvironmentSettings.SERVICE_PRINCIPAL_OBJECT_ID}&queryMembership=None'
        return cls._execute_get_request_on_vssps(endpoint)['value'][0]['id']

    @classmethod
    def create_project_with_pipelines(cls, project_name):
        """Create Project with Pipelines"""
        AdoProjectManager.create_project(project_name)
        AdoProjectManager.add_standard_branches(project_name)
        AdoProjectManager.write_files_to_repo_from_folder(project_name, 'dev', 'AdoStarter')
        AdoProjectManager.create_and_merge_pull_request(project_name, "dev", "test")
        AdoProjectManager.create_and_merge_pull_request(project_name, "test", "main")

        variable_group = AdoProjectManager.create_variable_group("environmental_variables", project_name)
        variable_group_id = variable_group['id']

        AdoProjectManager.create_pipeline(
            project_name,
            'Test Pipeline 1',
            '/.pipelines/azure-pipeline1.yml',
            variable_group_id)

        AdoProjectManager.create_pipeline(
            project_name,
            'Test Pipeline 2',
            '/.pipelines/azure-pipeline2.yml',
            variable_group_id)

        AdoProjectManager.create_pipeline(
            project_name,
            'Test Pipeline 3',
            '/.pipelines/azure-pipeline3.yml',
            variable_group_id)

        AppLogger.log_job_complete()