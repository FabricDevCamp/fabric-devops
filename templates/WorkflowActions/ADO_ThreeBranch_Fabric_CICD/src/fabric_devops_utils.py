"""Fabric DevOps Utility Classes"""

import base64
import shutil
import re

import os
import time
import datetime
import json
from json.decoder import JSONDecodeError
from typing import List

import requests
import msal

class EnvironmentSettings:
    """Environment Settings"""

    FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
    FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
    FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID")
    AUTHORITY = f'https://login.microsoftonline.com/{FABRIC_TENANT_ID}'
    FABRIC_CAPACITY_ID = os.getenv("FABRIC_CAPACITY_ID")

    WORKSPACE_ID_DEV = os.getenv("WORKSPACE_ID_DEV")
    WORKSPACE_ID_TEST = os.getenv("WORKSPACE_ID_TEST")
    WORKSPACE_ID_PROD = os.getenv("WORKSPACE_ID_PROD")

    ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
    DEVELOPERS_GROUP_ID = os.getenv("DEVELOPERS_GROUP_ID")
    
    ADO_PROJECT_NAME = os.getenv("BUILD_REPOSITORY_NAME")

    FABRIC_REST_API_RESOURCE_ID = 'https://api.fabric.microsoft.com'
    FABRIC_REST_API_BASE_URL = 'https://api.fabric.microsoft.com/v1/'
    POWER_BI_REST_API_BASE_URL = 'https://api.powerbi.com/v1.0/myorg/'
    
    DEPLOYMENT_JOBS = {
        'dev': {
            'name': 'dev',
            'web_datasource_path': 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Dev/',
            'adls_server': 'https://fabricdevcamp.dfs.core.windows.net/',
            'adls_container_name': 'sampledata', 
            'adls_container_path': '/ProductSales/Prod',            
        },
        'test': {
            'name': 'test',
            'web_datasource_path': 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Test/',
            'adls_server': 'https://fabricdevcamp.dfs.core.windows.net/',
            'adls_container_name': 'sampledata', 
            'adls_container_path': '/ProductSales/Test',
        },
        "prod": {
            'name': 'prod',
            'web_datasource_path': 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Prod/',
            'adls_server': 'https://fabricdevcamp.dfs.core.windows.net/',
            'adls_container_name': 'sampledata',
            'adls_container_path': '/ProductSales/Prod',
        }
    }
    
class AppLogger:
    """Logic to write log output to console and/or logs"""

    @classmethod
    def clear_console(cls):
        """Clear Console Window when running locally"""
        if os.name == 'nt':
            os.system('cls')

    @classmethod
    def log_job(cls, message):
        """start job"""
        print('', flush=True)
        print('|' + ('-' * (len(message) + 2)) + '|', flush=True)
        print(f'| {message} |', flush=True)
        print('|' + ('-' * (len(message) + 2)) + '|', flush=True)

    @classmethod
    def log_job_ended(cls, message=''):
        """log that job has ended"""
        print(' ', flush=True)
        print(f'> {message}', flush=True)
        print(' ', flush=True)

    @classmethod
    def log_job_complete(cls, workspace_id=None):
        """log that job has ended"""
        cls.log_step("Deployment job completed")
        if workspace_id is not None:
            workspace_launch_url = (
                f'https://app.powerbi.com/groups/{workspace_id}/list?experience=fabric-developer'
            )
            cls.log_substep(f'Workspace launch URL: {workspace_launch_url}')
        print(' ', flush=True)

    @classmethod
    def log_step(cls, message):
        """log a step"""
        print(' ', flush=True)
        print('> ' + message, flush=True)

    @classmethod
    def log_substep(cls, message):
        """log a sub step"""
        print('  - ' + message, flush=True)

    @classmethod
    def log_step_complete(cls):
        """add linebreak to log"""
        print(' ', flush=True)

    TABLE_WIDTH = 120

    @classmethod
    def log_table_header(cls, table_title):
        """Log Table Header"""
        print(' ', flush=True)
        print(f'> {table_title}', flush=True)
        print('  ' + ('-' * (cls.TABLE_WIDTH)), flush=True)

    @classmethod
    def log_table_row(cls, column1_value, column2_value):
        """Log Table Row"""
        column1_width = 20
        column1_value_length = len(column1_value)
        column1_offset = column1_width - column1_value_length
        column2_width = cls.TABLE_WIDTH - column1_width
        column2_value_length = len(column2_value)
        column2_offset = column2_width - column2_value_length - 5
        row = f'  | {column1_value}{" " * column1_offset}| {column2_value}{" " * column2_offset}|'
        print(row, flush=True)
        print('  ' + ('-' * (cls.TABLE_WIDTH)), flush=True)

    @classmethod
    def log_raw_text(cls, text):
        """log raw text"""
        print(text, flush=True)
        print('', flush=True)

    @classmethod
    def log_error(cls, message):
        """log error"""
        error_message = "ERROR: " + message
        print('-' * len(error_message), flush=True)
        print(error_message, flush=True)
        print('-' * len(error_message), flush=True)
        
        """Module to manage calls to Fabric REST APIs"""

class AdoProjectManager:
    """Wrapper class for calling Azure REST APIs for Azure Dev Ops"""

    ADO_ORGANIZATION = 'FabricDevCamp'
    BASE_URL = f'https://dev.azure.com/{ADO_ORGANIZATION}/_apis/'

    ADO_PROJECT_TEMPLATE_ID = "b8a3a935-7e91-48b8-a94c-606d37c3e9f2";
    ADO_API_VERSION = "api-version=7.1-preview";

    #region Low-level details about authentication and HTTP requests and responses

    # in-memory token cache
    _token_cache = dict()

    @classmethod
    def _get_ado_access_token(cls):
        ado_resource_id = '499b84ac-1321-427f-aa17-267ca6975798'
        scope = ado_resource_id + "/.default"
    
        if (scope in cls._token_cache) and \
            (datetime.datetime.now() < cls._token_cache[scope]['access_token_expiration']):
            return cls._token_cache[scope]['access_token']
                    
        app = msal.ConfidentialClientApplication(
                EnvironmentSettings.FABRIC_CLIENT_ID,
                authority=EnvironmentSettings.AUTHORITY,
                client_credential=EnvironmentSettings.FABRIC_CLIENT_SECRET)

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
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
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
        
class FabricRestApi:
    """Wrapper class for calling Fabric REST APIs"""

    #region 'Low-level details about authentication and HTTP requests and responses'

    # in-memory token cache
    _token_cache = dict()

    @classmethod
    def _get_fabric_access_token(cls):
      scope = EnvironmentSettings.FABRIC_REST_API_RESOURCE_ID + "//.default"
      if (scope in cls._token_cache) and \
         (datetime.datetime.now() < cls._token_cache[scope]['access_token_expiration']):
           return cls._token_cache[scope]['access_token']
                
      app = msal.ConfidentialClientApplication(
            EnvironmentSettings.FABRIC_CLIENT_ID,
            authority=EnvironmentSettings.AUTHORITY,
            client_credential=EnvironmentSettings.FABRIC_CLIENT_SECRET)

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
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            AppLogger.log_step(f'Waiting {wait_time} seconds due to 429 TOO MANY REQUESTS error')
            time.sleep(wait_time)
            return cls._execute_get_request(endpoint)
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request(cls, endpoint, post_body=''):
        """Execute POST request with support for Long-running Operations (LRO)"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}

        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201, 204 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code == 202:
            operation_state_url = response.headers.get('Location')
            wait_time = 10 # int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            response = requests.get(url=operation_state_url, headers=request_headers, timeout=60)
            operation_state = response.json()
            while operation_state['status'] != 'Succeeded' and \
                    operation_state['status'] != 'Failed':
                time.sleep(wait_time)
                response = requests.get(url=operation_state_url,
                                        headers=request_headers,
                                        timeout=60)
                operation_state = response.json()

            if operation_state['status'] == 'Succeeded':
                if 'Location' in response.headers:
                    operation_result_url = response.headers.get('Location')
                    response = requests.get(url=operation_result_url,
                                            headers=request_headers,
                                            timeout=60)
                    if response.status_code == 200:
                        return response.json()
                    else:
                        AppLogger.log_error(f"Error - {response.status_code}")
                        return None
                else:
                    return None
            else:
                AppLogger.log_error(f"Error - {operation_state}")

        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
     
        elif response.status_code == 404: # handleNOT FOUND error
            AppLogger.log_substep('Received 404 NOT FOUND error - waiting 10 seconds and retrying')
            wait_time = 10
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
        
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            raise RuntimeError(f'Error executing POST request: {response.status_code} - {response.text}')

    @classmethod
    def _execute_post_request_for_capacity_assignment(cls, endpoint, post_body=''):
        """Execute POST request with support without support for Long-running Operations (LRO)"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}

        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code == 202:
            return None

        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
     
        elif response.status_code == 404: # handleNOT FOUND error
            AppLogger.log_substep('Received 404 NOT FOUND error - waiting 10 seconds and retrying')
            wait_time = 10
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
        
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            raise RuntimeError(f'Error executing POST request: {response.status_code} - {response.text}')

    @classmethod
    def _execute_post_request_for_job_scheduler(cls, endpoint, post_body=''):
        """Execute POST request with support for Om-demand Job with Job Scheduler"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.post(url=rest_url, headers=request_headers, json=post_body, timeout=60)

        if response.status_code == 202:
            operation_state_url = response.headers.get('Location')
            wait_time = 10 # int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            response = requests.get(url=operation_state_url, headers=request_headers, timeout=60)
            operation_state = response.json()
            while operation_state['status'] == 'NotStarted' or \
                    operation_state['status'] == 'InProgress' or \
                    (operation_state['status'] == 'Failed' and \
                     operation_state['failureReason']['errorCode'] == 'RequestExecutionFailed'    ):
                time.sleep(wait_time)
                response = requests.get(url=operation_state_url,
                                        headers=request_headers,
                                        timeout=60)
                operation_state = response.json()

            if operation_state['status'] == 'Completed':
                return

            if operation_state['status'] == 'Failed':
                AppLogger.log_error('On-demand job Failed')
                print('----------------------------')
                print(operation_state)
                print('----------------------------')
                print(response)
                print('----------------------------')
                raise RuntimeError('On-demand job started but Failed')

            if operation_state['status'] == 'Cancelled':
                AppLogger.log_error('On-demand job was cancelled')

            if operation_state['status'] == 'Deduped':
                AppLogger.log_error('On-demand job was depuped')

        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            cls._execute_post_request_for_job_scheduler(endpoint, post_body)
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')

    @classmethod
    def _execute_patch_request(cls, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.patch(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code in {200, 204}:
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
    def _execute_delete_request(cls, endpoint):
        """Execute DELETE Request on Fabric REST API Endpoint"""
        rest_url = EnvironmentSettings.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers= {'Content-Type':'application/json',
                            'Authorization': f'Bearer {access_token}'}
        response = requests.delete(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return None
        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            cls._execute_delete_request(endpoint)
            return None
        else:
            AppLogger.log_error(
                f'Error executing DELETE request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_get_request_to_powerbi(cls, endpoint):
        """Execute GET Request on Power BI REST API Endpoint"""

        rest_url = EnvironmentSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code in { 200, 202 }:
            return response.json()
        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            AppLogger.log_substep(f'Waiting {wait_time} seconds due to 429 TOO MANY REQUESTS error')
            time.sleep(wait_time)
            return cls._execute_get_request_to_powerbi(endpoint)
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request_to_powerbi(cls, endpoint, post_body=''):
        rest_url = EnvironmentSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        return requests.post(url=rest_url, headers=request_headers, json=post_body, timeout=60)

    @classmethod
    def _execute_patch_request_to_powerbi(cls, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = EnvironmentSettings.POWER_BI_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.patch(url=rest_url, json=post_body, headers=request_headers, timeout=60)
        if response.status_code in {200, 204}:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_patch_request_to_powerbi(endpoint, post_body)
        else:
            AppLogger.log_error(
                f'Error executing PATCH request: {response.status_code} - {response.text}')
            return None

    #endregion

    @classmethod
    def list_capacities(cls):
        """Get all workspaces accessible to caller"""
        return cls._execute_get_request('capacities')['value']

    @classmethod
    def display_capacities(cls):
        """Display all capacities accessible to caller"""
        capacities = cls.list_capacities()
        AppLogger.log_step('Capacities:')
        for capacity in capacities:
            capacity_item = \
                f"[ id={capacity['id']}, sku={capacity['sku']}, displayName={capacity['displayName']} ] "
            AppLogger.log_substep(capacity_item)
        AppLogger.log_step_complete()

    @classmethod
    def list_workspaces(cls):
        """Get all workspaces accessible to caller"""
        all_workspaces = cls._execute_get_request('workspaces')['value']
        return filter(lambda workspace: workspace['type'] == 'Workspace', all_workspaces)

    @classmethod
    def display_workspaces(cls):
        """Display all workspaces accessible to caller"""
        workspaces = cls.list_workspaces()
        AppLogger.log_step('Workspaces:')
        for workspace in workspaces:
            workspace_item = f"[ id={workspace['id']}, displayName={workspace['displayName']} ]"
            AppLogger.log_substep(workspace_item)
        AppLogger.log_step_complete()

    @classmethod
    def get_workspace_info(cls, workspace_id):
        """Get Workspace information by ID"""
        return cls._execute_get_request(f'workspaces/{workspace_id}')

    @classmethod
    def get_workspace_by_name(cls, display_name):
        """Get Workspace item by display name"""
        workspaces =    cls.list_workspaces()
        for workspace in workspaces:
            if workspace['displayName'] == display_name:
                return workspace
        return None

    @classmethod
    def create_workspace(cls, display_name, capacity_id = None):
        """Create a new Fabric workspace"""
        AppLogger.log_step(f'Creating workspace [{display_name}]')

        if capacity_id is None:
            capacity_id = EnvironmentSettings.FABRIC_CAPACITY_ID

        existing_workspace = cls.get_workspace_by_name(display_name)
        if existing_workspace is not None:
            AppLogger.log_substep('Deleting existing workspace with same name')
            cls.delete_workspace(existing_workspace['id'])

        post_body = {
            'displayName': display_name,
            'description': 'Demo workspace created by API',
            'capacityId': capacity_id
        }

        AppLogger.log_substep('Calling [Create Workspace] API...')
        workspace = cls._execute_post_request('workspaces', post_body)
        workspace_id = workspace['id']
        AppLogger.log_substep(f'Workspace created with Id of [{workspace_id}]')

        return workspace

    @classmethod
    def assign_workspace_to_capacity(cls, workspace_id, capacity_id):
        """Create a new Fabric workspace"""
        AppLogger.log_step(f'Assigning workspace to capacity [{capacity_id}]')

        rest_url = f'workspaces/{workspace_id}/assignToCapacity'
        post_body = {
            'capacityId': capacity_id
        }

        cls._execute_post_request_for_capacity_assignment(rest_url, post_body)

        workspace_info = cls.get_workspace_info(workspace_id)

        AppLogger.log_substep(f"Capacity assignment {workspace_info['capacityAssignmentProgress']}")

        while workspace_info['capacityAssignmentProgress'] != 'Completed':
            time.sleep(10)
            workspace_info = cls.get_workspace_info(workspace_id)
            AppLogger.log_substep(f"Capacity assignment {workspace_info['capacityAssignmentProgress']}")
      
        AppLogger.log_substep(f'Workspace has been successfully assigned to capacity [{capacity_id}]')

    @classmethod
    def delete_workspace(cls, workspace_id):
        """Delete Workspace"""
        AppLogger.log_substep("Deleting workspace")
        workspace_connections    = cls.list_workspace_connections(workspace_id)
        for connection in workspace_connections:
            AppLogger.log_substep(f"Deleting connection {connection['displayName']}")
            cls.delete_connection(connection['id'])

        if cls.workspace_has_provisioned_identity(workspace_id):
            cls.deprovision_workspace_identity(workspace_id)

        endpoint = f'workspaces/{workspace_id}'
        cls._execute_delete_request(endpoint)

    @classmethod
    def update_workspace_description(cls, workspace_id, description):
        """Update Workspace Description"""
        endpoint = f'workspaces/{workspace_id}'
        body = { 'description': description }
        cls._execute_patch_request(endpoint, body)

    @classmethod
    def add_workspace_user(cls, workspace_id, user_id, role_assignment):
        """Add workspace role for user"""
        endpoint =    'workspaces/' + workspace_id + '/roleAssignments'
        post_body = {
            'role': role_assignment,
            'principal': {
            'id': user_id,
            'type': 'User'
            }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def add_workspace_group(cls, workspace_id, group_id, role_assignment):
        """Add workspace role for group"""
        endpoint =    'workspaces/' + workspace_id + '/roleAssignments'
        post_body = {
            'role': role_assignment,
            'principal': {
            'id': group_id,
            'type': 'Group'
            }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def add_workspace_spn(cls, workspace_id, spn_id, role_assignment):
        """Add workspace role for user"""
        endpoint =    'workspaces/' + workspace_id + '/roleAssignments'
        post_body = {
            'role': role_assignment,
            'principal': {
            'id': spn_id,
            'type': 'ServicePrincipal'
            }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def provision_workspace_identity(cls, workspace_id, workspace_role = 'Admin'):
        """Provision Workspace Identity"""
        AppLogger.log_substep('Provisioning workspace identity')
        rest_url = f"workspaces/{workspace_id}/provisionIdentity"
        workspace_identity = cls._execute_post_request(rest_url)
        service_principal_id = workspace_identity['servicePrincipalId']
        AppLogger.log_substep(f'Workspace identity provisioned with service principal id [{service_principal_id}]')
        cls.add_workspace_spn(workspace_id, service_principal_id, workspace_role)
        AppLogger.log_substep(f'Workspace identity added to workspace role of [{workspace_role}]')

    @classmethod
    def workspace_has_provisioned_identity(cls, workspace_id):
        """Check if Workspace has a provisioned identity"""
        workspace_info = cls.get_workspace_info(workspace_id)
        return 'workspaceIdentity' in workspace_info

    @classmethod
    def deprovision_workspace_identity(cls, workspace_id):
        """Deprovision Workspace Identity"""
        AppLogger.log_substep('Deprovisioning workspace identity')
        rest_url = f"workspaces/{workspace_id}/deprovisionIdentity"
        cls._execute_post_request(rest_url)

    @classmethod
    def get_workspace_spark_settings(cls, workspace_id):
        """Get Spark Settings for a workspace"""
        rest_url = f"workspaces/{workspace_id}/spark/settings"
        return cls._execute_get_request(rest_url)

    @classmethod
    def list_connections(cls):
        """Get all connections accessible to caller"""
        response = cls._execute_get_request('connections')
        return response['value']

    @classmethod
    def list_supported_connection_types(cls):
        """Get all connections accessible to caller"""
        response = cls._execute_get_request('connections/supportedConnectionTypes')
        return response['value']

    @classmethod
    def get_connection_by_name(cls, display_name):
        """Get Connection By Name"""
        connections = cls.list_connections()
        for connection in connections:
            if connection['displayName'] == display_name:
                return connection
        return None

    @classmethod
    def display_connections(cls):
        """Get all connections accessible to caller"""
        connections = cls.list_connections()
        AppLogger.log_step('Connections:')
        for connection in connections:
            AppLogger.log_substep(
                f"{connection['id']} - {connection['displayName']}")

    @classmethod
    def create_connection(cls, create_connection_request, top_level_step = False):
        """ Create new connection"""
        log_message = f"Creating {create_connection_request['connectionDetails']['type']} " + \
                        f"connection named {create_connection_request['displayName']} ..."

        if top_level_step:
            AppLogger.log_step(log_message)
        else:
            AppLogger.log_substep(log_message)

        existing_connections = cls.list_connections()
        for connection in existing_connections:
            if 'displayName' in connection and \
                connection['displayName'] == create_connection_request['displayName']:
                AppLogger.log_substep(
                    f"Using existing Connection with id [{connection['id']}]")
                return connection

        connection = cls._execute_post_request('connections', create_connection_request)

        AppLogger.log_substep(
            f"Connection created with id [{connection['id']}]")

        AppLogger.log_substep('Adding connection role of [Owner] for user')
        cls.add_connection_role_assignment_for_user(connection['id'],
                                                    EnvironmentSettings.ADMIN_USER_ID,
                                                    'Owner')


        return connection

    @classmethod
    def add_connection_role_assignment_for_user(cls, connection_id, admin_user_id, connection_role):
        """Add connection role assignment for user"""
        rest_url = f'connections//{connection_id}//roleAssignments'
        post_body = {
            'principal': {
                'id': admin_user_id,
                'type': "User"
            },
            'role': connection_role
        }
        return cls._execute_post_request(rest_url, post_body)

    @classmethod
    def add_connection_role_assignment_for_spn(cls, connection_id, spn_object_id, connection_role):
        """Add connection role assignment for user"""
        rest_url = f'connections//{connection_id}//roleAssignments'
        post_body = {
            'principal': {
                'id': spn_object_id,
                'type': "ServicePrincipal"
            },
            'role': connection_role
        }
        return cls._execute_post_request(rest_url, post_body)

    @classmethod
    def create_anonymous_web_connection(cls, web_url, workspace = None):
        """Create new Web connection using Anonymous credentials"""
        display_name = f'Web-Anonymous-[{web_url}]'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'Web',
                'creationMethod': 'Web',
                'parameters': [ 
                    {
                        'value': web_url,
                        'dataType': 'Text',
                        'name': 'url'
                    }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'credentialType': 'Anonymous' 
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_sql_connection_with_service_principal(cls, server, database,
                                                     workspace = None, lakehouse = None):
        """Create SQL Connection"""
        display_name = ''
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name
            if lakehouse is not None:
                display_name = display_name + f"Lakehouse[{lakehouse['displayName']}]"
            else:
                display_name = display_name + 'SQL'
        else:
            display_name = f'SQL-{server}:{database}'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'SQL',
                'creationMethod': 'Sql',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': database, 'dataType': 'Text', 'name': 'database' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'tenantId': EnvironmentSettings.FABRIC_TENANT_ID,
                    'servicePrincipalClientId': EnvironmentSettings.FABRIC_CLIENT_ID,
                    'servicePrincipalSecret': EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_sql_connection_with_workspace_identity(cls, server, database,
                                                     workspace = None, lakehouse = None):
        """Create SQL Connection"""
        
        # provision workspace identity
        cls.provision_workspace_identity(workspace['id'])
        
        display_name = ''
        display_name = f"Workspace[{workspace['id']}]-" + display_name
        if lakehouse is not None:
            display_name = display_name + f"Lakehouse[{lakehouse['displayName']}]"
        else:
            display_name = display_name + 'SQL'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'SQL',
                'creationMethod': 'Sql',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': database, 'dataType': 'Text', 'name': 'database' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'credentialType': 'WorkspaceIdentity'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_proxy_connection_with_workspace_identity(cls, server, database, workspace):
        """Create AnalysisServices Connection for proxy model"""
        
        # provision workspace identity if needed
        if cls.workspace_has_provisioned_identity(workspace['id']) is False:
            cls.provision_workspace_identity(workspace['id'])
        
        display_name = ''
        display_name = f"Workspace[{workspace['id']}]-" + display_name
        display_name = display_name + f'AnalysisServices[{database}]'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AnalysisServices',
                'creationMethod': 'AnalysisServices',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': database, 'dataType': 'Text', 'name': 'database' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'credentialType': 'WorkspaceIdentity'
                },
                'singleSignOnType': 'MicrosoftEntraID',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)

    @classmethod
    def create_azure_storage_connection_with_account_key(cls, server, path,
                                                         workspace = None,
                                                         top_level_step = False):
        """Create Azure Storage connections"""

        display_name = 'ADLS'
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDataLakeStorage',
                'creationMethod': 'AzureDataLakeStorage',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': path, 'dataType': 'Text', 'name': 'path' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'key': '', # EnvironmentSettings.AZURE_STORAGE_ACCOUNT_KEY,
                    'credentialType': 'Key'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request, top_level_step=top_level_step)

    @classmethod
    def create_azure_storage_connection_with_service_principal(cls, server, path,
                                                         workspace = None, lakehouse = None,
                                                         top_level_step = False):
        """Create Azure Storage connections"""

        display_name = ''
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name
            if lakehouse is not None:
                display_name = display_name + f"Lakehouse[{lakehouse['displayName']}]"
        else:
            display_name = f'ADLS-{server}:{path}'

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDataLakeStorage',
                'creationMethod': 'AzureDataLakeStorage',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': path, 'dataType': 'Text', 'name': 'path' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'tenantId': EnvironmentSettings.FABRIC_TENANT_ID,
                    'servicePrincipalClientId': EnvironmentSettings.FABRIC_CLIENT_ID,
                    'servicePrincipalSecret': EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request, top_level_step=top_level_step)

    @classmethod
    def create_azure_storage_connection_with_sas_token(cls, server, path,
                                                         workspace = None,
                                                         top_level_step = True):
        """Create Azure Storage connections"""

        display_name = 'ADLS'
        if workspace is not None:
            display_name = f"Workspace[{workspace['id']}]-" + display_name

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDataLakeStorage',
                'creationMethod': 'AzureDataLakeStorage',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': path, 'dataType': 'Text', 'name': 'path' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'token': EnvironmentSettings.AZURE_STORAGE_SAS_TOKEN,
                    'credentialType': 'SharedAccessSignature'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request, top_level_step=top_level_step)

    @classmethod
    def patch_oauth_connection_to_kqldb(cls, workspace, semantic_model, query_service_uri):
        """Patch AzureDataExplorer connections using OAuth credentials"""

        datasources = FabricRestApi.list_datasources_for_semantic_model(workspace['id'], semantic_model['id'])

        for datasource in datasources:
            if datasource['datasourceType'] == 'Extension' and \
                 datasource['connectionDetails']['kind'] == 'AzureDataExplorer':

                datasource_id = datasource['datasourceId']
                gateway_id = datasource['gatewayId']                
                rest_url = f'gateways/{gateway_id}/datasources/{datasource_id}'
                
                AppLogger.log_step(f'Patching credentials for {query_service_uri}')

                access_token = EntraIdTokenManager.get_kqldb_access_token(query_service_uri)

                part1 = r'{"credentialData":[{"name":"accessToken","value":"'
                part2 = r'"}]}'

                stringified_credentials = part1 + access_token + part2

                update_datasource_request = {
                    'credentialDetails': {
                        'credentialType': 'OAuth2',
                        'credentials': stringified_credentials,
                        'encryptedConnection': 'NotEncrypted',
                        'encryptionAlgorithm': 'None',
                        'privacyLevel': 'None',
                        'useCallerAADIdentity': 'False'
                    }
                }
         
                cls._execute_patch_request_to_powerbi(rest_url, update_datasource_request)
                AppLogger.log_substep("OAuth creds successfully patched")

    @classmethod
    def get_connection(cls, connection_id):
        """ Get connection using connection Id"""
        return cls._execute_get_request(f'connections//{connection_id}')

    @classmethod
    def delete_connection(cls, connection_id):
        """Delete connection using connection Id"""
        rest_url = f"connections/{connection_id}"
        return cls._execute_delete_request(rest_url)

    @classmethod
    def list_workspace_connections(cls, workspace_id):
        """Get connections associated with a specifc workspace"""
        workspace_connections = []
        connections = cls.list_connections()
        for connection in connections:
            if connection['displayName'] is not None and workspace_id in connection['displayName']:
                workspace_connections.append(connection)
        return workspace_connections

    @classmethod
    def list_folders(cls, workspace_id):
        """List Folder"""
        endpoint = f'workspaces/{workspace_id}/folders'
        return cls._execute_get_request(endpoint)['value']

    @classmethod
    def create_folder(cls, workspace_id, folder_name, parent_folder_id = None):
        """Create Folder"""
        endpoint = f'workspaces/{workspace_id}/folders'
        body = { 'displayName': folder_name }

        if parent_folder_id is not None:
            body['parentFolderId'] = parent_folder_id

        AppLogger.log_step(f'Creating folder {folder_name}')

        folder = cls._execute_post_request(endpoint, body)
        AppLogger.log_substep(f"folder creatrd with Id [{folder['id']}]")
        
        return folder

    @classmethod
    def create_item(cls, workspace_id, create_item_request, folder_id = None):
        """Create Item using create-item-request"""
        if folder_id is not None:
            create_item_request['folderId'] = folder_id

        AppLogger.log_step(
            f"Creating [{create_item_request['displayName']}.{create_item_request['type']}]...")
        endpoint = f'workspaces/{workspace_id}/items'
        item = cls._execute_post_request(endpoint, create_item_request)
        AppLogger.log_substep(f"{item['type']} created with id [{item['id']}]")
        return item

    @classmethod
    def update_item(cls, workspace_id, item, update_item_request):
        """Update Item Definition using update-item--definition-request"""
        AppLogger.log_step(f"Updating [{item['displayName']}.{item['type']}]...")
        endpoint = f"workspaces/{workspace_id}/items/{item['id']}"
        item = cls._execute_post_request(endpoint, update_item_request)
        AppLogger.log_substep(f"{item['type']} updated")
        return item

    @classmethod
    def update_item_definition(cls, workspace_id, item, update_item_definition_request):
        """Update Item Definition using update-item--definition-request"""
        AppLogger.log_substep(
            f"Updating [{item['displayName']}.{item['type']}]...")
        endpoint = f"workspaces/{workspace_id}/items/{item['id']}/updateDefinition"
        item = cls._execute_post_request(endpoint, update_item_definition_request)
        return item

    @classmethod
    def get_item_definition(cls, workspace_id, item, export_format = None):
        """Get Item Definition"""
        endpoint = f"workspaces/{workspace_id}/items/{item['id']}/getDefinition"
        if export_format is not None:
            endpoint = endpoint + f'?format={export_format}'
        return cls._execute_post_request(endpoint)

    @classmethod
    def export_item_definitions(cls, workspace_id):
        """Get Item Definition"""
        endpoint = f"workspaces/{workspace_id}/exportItemDefinitions"
        post_body    = {
            'mode': 'All'
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def purge_logical_ids(cls, workspace_id):
        """Get Item Definition"""
        endpoint = f"workspaces/{workspace_id}/__private/purgeLogicalIds"
        return cls._execute_post_request(endpoint)

    @classmethod
    def import_item_definitions(cls, workspace_id, definition_parts):
        """Get Item Definition"""
        endpoint = f"workspaces/{workspace_id}/importItemDefinitions"
        post_body    = {
            'definitionParts': definition_parts
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def copy_workspace_items(cls, source_workspace_name, target_workspace_name):        
        """Get Item Definition"""

        source_workspace = cls.get_workspace_by_name(source_workspace_name)
        target_workspace = cls.get_workspace_by_name(target_workspace_name) 

        if target_workspace is None:
            target_workspace = cls.create_workspace(target_workspace_name)

        source_export = cls.export_item_definitions(source_workspace['id'])

        definition_parts = source_export['DefinitionParts']

        AppLogger.log_step("Importing items")
        cls.import_item_definitions(target_workspace['id'] , definition_parts)
        AppLogger.log_substep("items imported")

    @classmethod
    def create_lakehouse(cls, workspace_id, display_name, folder_id = None, enable_schemas = False):
        """Create Lakehouse"""
        create_item_request = {
            'displayName': display_name, 
            'type': 'Lakehouse' 
        }

        if enable_schemas:
            create_item_request['creationPayload'] = { 'enableSchemas': True }
        return cls.create_item(workspace_id, create_item_request, folder_id)

    @classmethod
    def create_warehouse(cls, workspace_id, display_name, folder_id = None):
        """Create Warehouse"""
        create_item_request = {
            'displayName': display_name, 
            'type': 'Warehouse' 
        }
        return cls.create_item(workspace_id, create_item_request, folder_id)

    @classmethod
    def create_sql_database(cls, workspace_id, display_name, folder_id = None):
        """Create Lakehouse"""
        create_item_request = {
            'displayName': display_name, 
            'type': 'SQLDatabase' 
        }

        return cls.create_item(workspace_id, create_item_request, folder_id)

    @classmethod
    def list_workspace_items(cls, workspace_id, item_type = None):
        """Get items in workspace"""
        endpoint = f'workspaces/{workspace_id}/items'

        if item_type is not None:
            endpoint = endpoint + f'?type={item_type}'

        return cls._execute_get_request(endpoint)['value']

    @classmethod
    def get_item_by_name(cls, workspace_id, display_name, item_type):
        """Get Item by Name"""
        items = cls.list_workspace_items(workspace_id, item_type)
        for item in items:
            if item['displayName'] == display_name:
                return item
        return None

    @classmethod
    def run_notebook(cls, workspace_id, notebook):
        """Run notebook and wait for job completion"""
        AppLogger.log_substep(f"Running notebook [{notebook['displayName']}]...")
        rest_url = f"workspaces/{workspace_id}/items/{notebook['id']}" + \
                    "/jobs/instances?jobType=RunNotebook"
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Notebook run job completed successfully")
        return response

    @classmethod
    def run_data_pipeline(cls, workspace_id, pipeline):
        """Run notebook and wait for job completion"""
        AppLogger.log_substep(f"Running data pipeline [{pipeline['displayName']}]...")

        rest_url = f"workspaces/{workspace_id}/items/{pipeline['id']}" + \
                    "/jobs/instances?jobType=Pipeline"
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Data pipeline run job completed successfully")
        return response

    @classmethod
    def run_copyjob(cls, workspace_id, copyjob):
        """Run CopyJob and wait for job completion"""
        AppLogger.log_substep(f"Running CopyJob [{copyjob['displayName']}]...")

        rest_url = f"workspaces/{workspace_id}/items/{copyjob['id']}" + \
                    "/jobs/instances?jobType=Execute"
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("CopyJob run job completed successfully")
        return response

    @classmethod
    def apply_changes_to_dataflow(cls, workspace_id, dataflow):
        """Apply changes to dataflow"""
        AppLogger.log_substep(f"Applying changes to dataflow [{dataflow['displayName']}]...")

        rest_url = f"workspaces/{workspace_id}/dataflows/{dataflow['id']}" + \
                    "/jobs/instances?jobType=ApplyChanges"
        
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Dataflow changes applied successfully")
        return response

    @classmethod
    def run_dataflow(cls, workspace_id, dataflow):
        """Apply changes to dataflow"""
        AppLogger.log_substep(f"Running dataflow [{dataflow['displayName']}]...")

        rest_url = f"workspaces/{workspace_id}/dataflows/{dataflow['id']}" + \
                    "/jobs/instances?jobType=Execute"
        
        response = cls._execute_post_request_for_job_scheduler(rest_url)
        AppLogger.log_substep("Dataflow run successfully")
        return response

    @classmethod
    def create_notebook_schedule(cls, workspace_id, item, schedule_definition = None):
        """Create item schedule"""

        if schedule_definition is None:
            schedule_definition = {
                "enabled": True,
                "configuration": {
                    "startDateTime": date.today().strftime("%Y-%m-%d") + "T00:00:00",
                    "endDateTime": (date.today() + timedelta(days=90)).strftime("%Y-%m-%d") + "T00:00:00",
                    "localTimeZoneId": "Eastern Standard Time",
                    "type": "Daily",
                     "times": ["06:00:00", "17:30:00"]
                }
            }

        print( json.dumps(schedule_definition, indent=4) )

        AppLogger.log_substep(f"Scheduling [{item['type']}] [{item['displayName']}]...")
        rest_url = f"workspaces/{workspace_id}/items/{item['id']}" + \
                    "/jobs/RunNotebook/schedules"
        response = cls._execute_post_request(rest_url, schedule_definition)
        AppLogger.log_substep("Item schedule created successfully")
        return response

    @classmethod
    def get_sql_database_properties(cls, workspace_id, sql_database_id):
        """Get SQL database properties"""
        rest_url = f'workspaces/{workspace_id}/sqlDatabases/{sql_database_id}'
        return cls._execute_get_request(rest_url)['properties']

    @classmethod
    def get_lakehouse(cls, workspace_id, lakehouse_id):
        """Get lakehouse properties"""
        rest_url = f'workspaces/{workspace_id}/lakehouses/{lakehouse_id}'
        return cls._execute_get_request(rest_url)

    @classmethod
    def get_sql_endpoint_for_lakehouse(cls, workspace_id, lakehouse):
        """Get SQL endpoint properties for lakehouse"""

        lakehouse = cls.get_lakehouse(workspace_id, lakehouse['id'])
        while lakehouse['properties']['sqlEndpointProperties']['provisioningStatus'] != 'Success':
            wait_time = 10
            time.sleep(wait_time)
            lakehouse = cls.get_lakehouse(workspace_id, lakehouse['id'])

        server = lakehouse['properties']['sqlEndpointProperties']['connectionString']
        database = lakehouse['properties']['sqlEndpointProperties']['id']

        return {
            'server': server,
            'database': database
        }

    @classmethod
    def get_onelake_path_for_lakehouse(cls, workspace_id, lakehouse):
        """Get SQL endpoint properties for lakehouse"""

        AppLogger.log_step(
            f"Getting OneLake path for lakehouse [{lakehouse['displayName']}]...")

        lakehouse = cls.get_lakehouse(workspace_id, lakehouse['id'])

        onelake_path = lakehouse['properties']['oneLakeTablesPath'].replace('Tables', '')
        AppLogger.log_substep(f"OneLake path: {onelake_path}")

        return onelake_path

    @classmethod
    def refresh_sql_endpoint_metadata(cls, workspace_id, sql_endpoint_id):
        """Refresh SQL Endpoint"""
        AppLogger.log_substep("Updating SQL Endpoint metadata...")
        endpoint = \
            f"workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata?preview=True"
        cls._execute_post_request(endpoint, {})
        AppLogger.log_substep("SQL Endpoint metadata update complete")

    @classmethod
    def get_warehouse(cls, workspace_id, warehouse_id):
        """Get warehouse properties"""
        rest_url = f'workspaces/{workspace_id}/warehouses/{warehouse_id}'
        return cls._execute_get_request(rest_url)

    @classmethod
    def get_warehouse_connection_string(cls, workspace_id, warehouse_id):
        """Get warehouse properties"""
        rest_url = f'workspaces/{workspace_id}/warehouses/{warehouse_id}'
        warehouse = cls._execute_get_request(rest_url)
        return warehouse['properties']['connectionString']

    @classmethod
    def get_eventhouse(cls, workspace_id, eventhouse_id):
        """Get warehouse properties"""
        rest_url = f'workspaces/{workspace_id}/eventhouses/{eventhouse_id}'
        return cls._execute_get_request(rest_url)

    @classmethod
    def list_datasources_for_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Datasource for semantic model using Power BI REST API"""
        rest_url    = f'groups//{workspace_id}//datasets//{semantic_model_id}//datasources'
        return cls._execute_get_request_to_powerbi(rest_url)['value']

    @classmethod
    def get_web_url_from_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Web datasource URL from semantic model"""
        data_sources = cls.list_datasources_for_semantic_model(workspace_id, semantic_model_id)
        for data_source in data_sources:
            if data_source['datasourceType'] == 'Web':
                return data_source['connectionDetails']['url']

        return None

    @classmethod
    def get_datasources_for_semantic_model(cls, workspace_id, semantic_model_id):
        """Get datasources for semantic model"""
        return cls.list_datasources_for_semantic_model(workspace_id, semantic_model_id)

    @classmethod
    def get_sql_endpoint_from_semantic_model(cls, workspace_id, semantic_model_id):
        """Get Web datasource URL from semantic model"""
        data_sources = cls.list_datasources_for_semantic_model(workspace_id, semantic_model_id)
        for data_source in data_sources:
            if data_source['datasourceType'] == 'Sql':
                return data_source['connectionDetails']
        return None

    @classmethod
    def bind_semantic_model_to_connection(cls, workspace_id, semantic_model_id, connection_id):
        """Bind semantic model to connection"""
        rest_url    = f'groups//{workspace_id}//datasets//{semantic_model_id}//Default.BindToGateway'
        post_body    = {
            'gatewayObjectId': '00000000-0000-0000-0000-000000000000',
            'datasourceObjectIds': [ connection_id ]
        }
        cls._execute_post_request_to_powerbi(rest_url, post_body)

    @classmethod
    def refresh_semantic_model(cls, workspace_id, semantic_model_id):
        """Refresh semantic model"""
        AppLogger.log_substep('Refreshing semantic model...')
        rest_url    = f'groups//{workspace_id}//datasets//{semantic_model_id}//refreshes'
        post_body = { 'notifyOption': 'NoNotification', 'type': 'Automatic' }
        response = cls._execute_post_request_to_powerbi(rest_url, post_body)
        refresh_id = response.headers.get('x-ms-request-id')
        rest_url_refresh_details = \
            f'groups/{workspace_id}/datasets/{semantic_model_id}/refreshes/{refresh_id}'
        refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        while 'status' not in refresh_details or refresh_details['status'] == 'Unknown':
            time.sleep(6)
            refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        AppLogger.log_substep("Refresh operation complete")

    @classmethod
    def create_and_bind_semantic_model_connecton(cls, workspace,
                                                 semantic_model_id, lakehouse = None):
        """Create connection and bind it to semantic model"""
        datasources = cls.list_datasources_for_semantic_model(workspace['id'], semantic_model_id)
        for datasource in datasources:

            if datasource['datasourceType'].lower() == 'sql':
                AppLogger.log_substep('Creating SQL connection for semantic model')
                server = datasource['connectionDetails']['server']
                database = datasource['connectionDetails']['database']
                connection = cls.create_sql_connection_with_workspace_identity(server,
                                                                                database,
                                                                                workspace,
                                                                                lakehouse)
                AppLogger.log_substep('Binding semantic model to SQL connection')
                cls.bind_semantic_model_to_connection(workspace['id'],
                                                        semantic_model_id,
                                                        connection['id'])

            elif datasource['datasourceType'].lower() == 'web':
                AppLogger.log_substep('Creating Web connection for semantic model')
                web_url    = datasource['connectionDetails']['url']
                connection = cls.create_anonymous_web_connection(web_url, workspace)
                AppLogger.log_substep('Binding semantic model to Web connection')
                cls.bind_semantic_model_to_connection(workspace['id'],
                                                        semantic_model_id,
                                                         connection['id'])
                cls.refresh_semantic_model(workspace['id'], semantic_model_id)

            elif datasource['datasourceType'] == 'AzureDataLakeStorage':
                AppLogger.log_substep('Creating AzureDataLakeStorage connection for semantic model')
                server    = datasource['connectionDetails']['server']
                path      = datasource['connectionDetails']['path']
                connection = cls.create_azure_storage_connection_with_service_principal(server, path, workspace, lakehouse)
                AppLogger.log_substep('Binding semantic model to OneLake connection')
                cls.bind_semantic_model_to_connection(workspace['id'],
                                                        semantic_model_id,
                                                         connection['id'])
                cls.refresh_semantic_model(workspace['id'], semantic_model_id)
            else:
                AppLogger.log_substep(
                    f"Datasource type [{datasource['datasourceType']}] not supported")
                AppLogger.log_substep(
                    json.dumps( datasource, indent=4 ) )

    @classmethod
    def create_adls_gen2_shortcut(cls, workspace_id, lakehouse_id, name, path,
                                    location, subpath, connection_id):
        """Create ADLS Gen2 Shortcut"""
        AppLogger.log_step('Creating OneLake shortcut using ADLS connection...')

        rest_url = f'workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite'
        post_body = {
            'name': name,
            'path': path,
            'target': {
                'adlsGen2': {
                'location': location,
                'subpath': subpath,
                'connectionId': connection_id
                }
            }
        }
        cls._execute_post_request(rest_url, post_body)
        AppLogger.log_substep(f'Shortcut [{path}/{name}] successfullly created')

    @classmethod
    def create_adls_gen2_shortcut_with_variables(cls, workspace_id, lakehouse_id, name, path,
                                    location, subpath, connection_id):
        """Create ADLS Gen2 Shortcut"""
        AppLogger.log_step('Creating OneLake shortcut using ADLS connection...')

        rest_url = f'workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts'
        post_body = {
            'name': name,
            'path': path,
            'target': {
                'adlsGen2': {
                    'location': location,
                    'subpath': subpath,
                    'connectionId': connection_id,
                    'propertiesConfiguredByVariables': [
                        {
                             'property': 'target.adlsGen2.connectionId',
                             'variable': '$(/**/environment_settings/target_connection_id)'
                        },
                        {
                            'property': 'target.adlsGen2.location',
                            'variable': '$(/**/environment_settings/target_location)'
                        },
                        {
                            'property': 'target.adlsGen2.subpath',
                            'variable': '$(/**/environment_settings/target_subpath)'
                        }
                    ]
                }
            }
        }
        cls._execute_post_request(rest_url, post_body)
        AppLogger.log_substep(f'Shortcut [{path}/{name}] successfullly created')

    @classmethod
    def reset_adls_gen2_shortcut(cls, workspace_id, lakehouse_id, shortcut):
        """Reset ADLS Gen2 Shortcut"""
        cls.create_adls_gen2_shortcut(
            workspace_id,
            lakehouse_id,
            shortcut['name'],
            shortcut['path'],
            "$(/**/environment_settings/adls_server)",
            "$(/**/environment_settings/adls_shortcut_subpath)",
            "$(/**/environment_settings/adls_connection_id)"
        )     

    @classmethod
    def create_onelake_shortcut(cls, 
                                workspace_id, 
                                target_lakehouse_id,
                                source_lakehouse_id,
                                name, 
                                path,
                                ):
        """Create ADLS Gen2 Shortcut"""
        AppLogger.log_step('Creating OneLake shortcut using ADLS connection...')

        rest_url = f'workspaces/{workspace_id}/items/{target_lakehouse_id}/shortcuts'
        post_body = {
            'name': name,
            'path': f'/{path}',
            'target': {
                'onelake': {
                    'itemId': source_lakehouse_id,
                    'path': f'{path}/{name}',
                    'workspaceId': workspace_id
                }
            }
        }
        cls._execute_post_request(rest_url, post_body)
        AppLogger.log_substep(f'Shortcut [{path}/{name}] successfullly created')

    @classmethod
    def list_shortcuts(cls, workspace_id, lakehouse_id):
        """List Shortcuts"""
        rest_url = f'workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts'
        return cls._execute_get_request(rest_url)['value']

    @classmethod
    def set_active_valueset_for_variable_library(cls, workspace_id, library, valueset):
        """Set active valueset for variable library"""
        if valueset == 'dev':
            valueset = None
            
        AppLogger.log_substep(
            "Setting active valueset for variable library " + \
            f"[{library['displayName']}] to [{valueset}]...")

        rest_url = f"workspaces/{workspace_id}/VariableLibraries/{library['id']}"
        post_body = {
            'properties': {                
                'activeValueSetName': valueset
            }
        }
        cls._execute_patch_request(rest_url, post_body)
        
    @classmethod
    def initialize_git_connection(cls, workspace_id, initialize_connection_request):
        """Initialize GIT Connection"""
        endpoint = f"workspaces/{workspace_id}/git/initializeConnection"
        return cls._execute_post_request(endpoint, initialize_connection_request)

    @classmethod
    def get_git_status(cls, workspace_id):
        """Get GIT Connection Status"""
        endpoint = f"workspaces/{workspace_id}/git/status"
        return cls._execute_get_request(endpoint)

    @classmethod
    def get_git_connection(cls, workspace_id):
        """Get GIT Connection"""
        endpoint = f"workspaces/{workspace_id}/git/connection"
        return cls._execute_get_request(endpoint)

    @classmethod
    def disconnect_workspace_from_git(cls, workspace_id):
        """Disconnect Workspace from GIT Repository"""
        endpoint = f"workspaces/{workspace_id}/git/disconnect"
        return cls._execute_post_request(endpoint)

    @classmethod
    def commit_workspace_to_git(cls, workspace_id, commit_to_git_request = None, commit_comment = "commit workspace changes back to repo"):
        """Commit Workspace to GIT Repository"""
         
        AppLogger.log_substep("Committing Workspace content to GIT repository")
                
        if commit_to_git_request is None:
            currest_status = cls.get_git_status(workspace_id)
            changes = currest_status['changes']
            if len(changes) == 0:
                AppLogger.log_substep("There are no changes to push")
                return None
            commit_to_git_request = {
                    'mode': 'All',
                    'workspaceHead': currest_status['workspaceHead'],
                    'remoteCommitHash': currest_status['remoteCommitHash'],
                    'comment': commit_comment
            }
            
        endpoint = f"workspaces/{workspace_id}/git/commitToGit"
        response = cls._execute_post_request(endpoint, commit_to_git_request)
        AppLogger.log_substep('GIT sync process completed successfully')
        return response
    
    @classmethod
    def update_workspace_from_git(cls, workspace_id, update_from_git_request = None):
        """Update Workspace from GIT Repository"""
        AppLogger.log_substep("Pushing item definitions from GIT reposiitory to workspace items")
        
        if update_from_git_request is None:            
            git_status = FabricRestApi.get_git_status(workspace_id)
            update_from_git_request = {
                "workspaceHead": git_status['workspaceHead'],
                "remoteCommitHash": git_status['remoteCommitHash'],
                "conflictResolution": {
                    "conflictResolutionType": "Workspace",
                    "conflictResolutionPolicy": "PreferRemote"
                },
                "options": { "allowOverrideItems": True }                
            }
                    
        endpoint = f"workspaces/{workspace_id}/git/updateFromGit"
        response = cls._execute_post_request(endpoint, update_from_git_request)
        AppLogger.log_substep("GIT synchronization process complete")
        return response

    @classmethod
    def get_my_git_credentials(cls, workspace_id):
        """Get My GIT Credential"""
        endpoint = f"workspaces/{workspace_id}/git/myGitCredentials"
        return cls._execute_get_request(endpoint)

    @classmethod
    def update_my_git_credentials(cls, workspace_id, update_git_credentials_request):
        """Update My GIT Credentials"""
        endpoint = f"workspaces/{workspace_id}/git/myGitCredentials"
        return cls._execute_patch_request(endpoint, update_git_credentials_request)

    @classmethod
    def _create_ado_source_control_connection(cls, url, workspace, top_level_step = False):
        """Create GitHub connections with Personal Access Token"""

        display_name = f"ADO-GIT-SPN-[{url}]"

        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDevOpsSourceControl',
                'creationMethod': 'AzureDevOpsSourceControl.Contents',
                'parameters': [ 
                    { 'name': 'url', 'dataType': 'Text', 'value': url }
                ]
            },
             'credentialDetails': {
                'credentials': {
                    'tenantId': EnvironmentSettings.FABRIC_TENANT_ID,
                    'servicePrincipalClientId': EnvironmentSettings.FABRIC_CLIENT_ID,
                    'servicePrincipalSecret': EnvironmentSettings.FABRIC_CLIENT_SECRET,
                    'credentialType': 'ServicePrincipal'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request, top_level_step=top_level_step)
    
    @classmethod
    def _get_ado_repo_connection(cls, project_name, workspace):
        """Get Azure DevOps Repo Connection"""

        ado_repo_url = f'https://dev.azure.com/fabricdevcamp/{project_name}/_git/{project_name}/'

        connections = FabricRestApi.list_connections()

        for connection in connections:
            if connection['connectionDetails']['type'] == 'AzureDevOpsSourceControl' and \
                 connection['connectionDetails']['path'] == ado_repo_url:
                return connection

        return FabricRestApi._create_ado_source_control_connection(ado_repo_url, workspace)

    @classmethod
    def _create_workspace_connection_to_ado_repo_as_user(cls, workspace_id, project_name, branch = 'main'):
        
        """Connect Workspace Connection to Azure DevOps Repository"""

        endpoint = f"workspaces/{workspace_id}/git/connect"

        workspace_folder = "workspace"

        connect_request = {
            "gitProviderDetails": {
                "organizationName": "FabricDevCamp",
                "projectName": project_name,
                "gitProviderType": "AzureDevOps",
                "repositoryName": project_name,
                "branchName": branch,
                "directoryName": f"/{workspace_folder}"
            }
        }

        return cls._execute_post_request(endpoint, connect_request)

    @classmethod
    def _create_workspace_connection_to_ado_as_service_prinicpal(cls, workspace, project_name, branch = 'main'):
        """Connect Workspace Connection to Azure DevOps Repository"""

        workspace_id = workspace['id']

        endpoint = f"workspaces/{workspace_id}/git/connect"
        workspace_folder = "workspace"

        connection = cls._get_ado_repo_connection(project_name, workspace)
        connection_id = connection['id']

        connect_request = {
            "gitProviderDetails": {
                "organizationName": "FabricDevCamp",
                "projectName": project_name,
                "gitProviderType": "AzureDevOps",
                "repositoryName": project_name,
                "branchName": branch,
                "directoryName": f"/{workspace_folder}"
            },
            "myGitCredentials": {
                "source": "ConfiguredConnection",
                "connectionId": connection_id            
            }
        }

        return cls._execute_post_request(endpoint, connect_request)

    @classmethod
    def connect_workspace_to_ado_repo(cls, workspace, project_name, branch = 'main'):
        """Connect Workspace to Azure Dev Ops Repository"""

        AppLogger.log_substep(f"Connecting workspace[{workspace['displayName']}] " + \
                                f"to branch[{branch}] in Azure DevOps repo[{project_name}]")

        cls._create_workspace_connection_to_ado_as_service_prinicpal(workspace, project_name, branch)

        AppLogger.log_substep("Workspace connection created successfully")


        init_request = {
            'initializationStrategy': 'PreferWorkspace'
        }

        init_response = cls.initialize_git_connection(workspace['id'], init_request)

        required_action = init_response['requiredAction']

        if required_action == 'CommitToGit':
            commit_to_git_request = {
                'mode': 'All',
                'workspaceHead': init_response['workspaceHead'],
                'comment': 'Initial commit from workspace'
            }
            cls.commit_workspace_to_git(
                workspace['id'], 
                commit_to_git_request,
                '')

        if required_action == 'UpdateFromGit':
            update_from_git_request = {
                "workspaceHead": init_response['workspaceHead'],
                "remoteCommitHash": init_response['remoteCommitHash'],
                "conflictResolution": {
                    "conflictResolutionType": "Workspace",
                    "conflictResolutionPolicy": "PreferWorkspace"
                },
                "options": {
                    "allowOverrideItems": True
                }
                            
            }
            cls.update_workspace_from_git(workspace['id'], update_from_git_request)

        AppLogger.log_substep("Workspace connection successfully created and synchronized")

class Variable:
    """Variable in Variable Library"""
    name: str
    value: str
    type: str
    note: str

    def __init__(self, name: str, value: str, variable_type: str = 'String', note: str = '') -> None:
        self.name = name
        self.value = value
        self.type = variable_type
        self.note = note

    def encode(self):
        """encode support"""
        return self.__dict__

class VariableOverride:
    """Variable in Variable Library"""
    name: str
    value: str

    def __init__(self, name: str, value: str) -> None:
        self.name = name
        self.value = value

    def encode(self):
        """encode support"""
        return self.__dict__

class Valueset:
    """Variable Library"""
    variableOverrides: List[VariableOverride]

    def __init__(self, name):
        self.name = name
        self.variableOverrides = []

    def encode(self):
        """encode support"""
        return self.__dict__
    
    def add_variable_override(self, name: str, value: str):
        """"add variable"""
        self.variableOverrides.append( VariableOverride(name, value) )

class VariableLibrary:
    """Variable Library"""
    variables: List[Variable]
    valuesets: List[Valueset]

    def __init__(self, variables = None, valuesets = None):
        self.variables = variables if variables is not None else []
        self.valuesets = valuesets if valuesets is not None else []

    def encode(self):
        """encode support"""
        return self.__dict__

    def add_variable(self, name: str, value: str, variable_type: str = 'String', note: str = 'some note'):
        """"add variable"""
        self.variables.append( Variable(name, value, variable_type, note) )

    def add_valueset(self, valueset: Valueset):
        """"add valueset"""
        self.valuesets.append( valueset )


    def get_variable_json(self):
        "Get JSON for variables.json"
        variable_lib = {
            '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json',
            'variables': self.variables
        }
        return json.dumps(variable_lib, default=lambda o: o.encode(), indent=4)

    def get_valueset_json(self, valueset_name):
        """Get JSON for valueset.json"""
        valueset_list = list(filter(lambda valueset: valueset.name == valueset_name, self.valuesets))
        valueset: Valueset = valueset_list[0]
               
        valueset_export = {
            '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/valueSet/1.0.0/schema.json',
            'name': valueset.name,
            'variableOverrides': valueset.variableOverrides
        }
        return json.dumps(valueset_export, default=lambda o: o.encode(), indent=4)

class ItemDefinitionFactory:
    """Logic to to create and update item definitions"""

    @classmethod
    def _create_inline_base64_part(cls, path, payload):
        """create item definition part with base64 encoding"""
        return {
            'path': path,
            'payload': base64.b64encode(payload.encode('utf-8')).decode('utf-8'),
            'payloadType': 'InlineBase64'
        }

    @classmethod
    def _get_part_path(cls, item_folder_path, file_path):
        """get path for item definition part"""
        offset = file_path.find(item_folder_path) + len(item_folder_path) + 1
        return file_path[offset:].replace('\\', '/')

    @classmethod
    def _search_and_replace_in_payload(cls, payload, search_replace_text):
        payload_bytes = base64.b64decode(payload)
        payload_content = payload_bytes.decode('utf-8')
        for entry in search_replace_text.keys():
            payload_content = payload_content.replace(entry, search_replace_text[entry])
        return base64.b64encode(payload_content.encode('utf-8')).decode('utf-8')

    @classmethod
    def _search_and_replace_in_payload_with_regex(cls, payload, search_replace_terms):
        payload_bytes = base64.b64decode(payload)
        payload_content = payload_bytes.decode('utf-8')

        for search, replace in search_replace_terms.items():
            payload_content, count = re.subn(search, replace, payload_content)

        return base64.b64encode(payload_content.encode('utf-8')).decode('utf-8')

    @classmethod
    def get_template_file(cls, path):
        """get contents of a file from templates folder"""
        file_path = f".//templates//ItemDefinitionTemplateFiles//{path}"
        file = open(file_path,'r', encoding="utf-8")
        file_content = file.read()
        file.close()
        return file_content

    @classmethod
    def get_create_item_request_from_folder(cls, item_folder):
        """generate create item request from folder"""
        folder_path = f".//templates//ItemDefinitionTemplateFolders//{item_folder}"
        platform_file_path = f'{folder_path}//.platform'
        file = open(platform_file_path,'r', encoding="utf-8")
        file_content = json.loads(file.read())
        file.close()
        item_type = file_content['metadata']['type']
        item_display_name = file_content['metadata']['displayName']

        item_definition_parts = []

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                part_path = cls._get_part_path(item_folder, file_path)
                file = open(file_path,'r', encoding="utf-8")
                part_content = file.read()
                item_definition_parts.append(cls._create_inline_base64_part(part_path, part_content))

        return {
            'displayName': item_display_name,
            'type': item_type,
            'definition': {
                'parts': item_definition_parts
            }

        }

    @classmethod
    def update_item_definition_part(cls, item_definition, part_path, search_replace_text):
        """Update Item Definition Part"""
        item_part = next((part for part in item_definition['parts'] if part['path'] == part_path), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            item_part['payload'] = cls._search_and_replace_in_payload(item_part['payload'], search_replace_text)
            item_definition['parts'].append(item_part)
        return item_definition

    @classmethod
    def update_item_definition_part_with_regex(cls, item_definition, part_path, search_replace_terms):
        """Update Item Definition Part"""
        item_part = next((part for part in item_definition['parts'] if part['path'] == part_path), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            item_part['payload'] = cls._search_and_replace_in_payload_with_regex(item_part['payload'], search_replace_terms)
            item_definition['parts'].append(item_part)
        return item_definition

    # @classmethod
    # def get_create_notebook_request_from_folder(cls, item_folder, workspace_id, lakehouse):
    #     """generate create item request from folder"""
    #     create_request = cls.get_create_item_request_from_folder(item_folder)
    #     notebook_redirects = {
    #         '{WORKSPACE_ID}': workspace_id,
    #         '{LAKEHOUSE_ID}': lakehouse['id'],
    #         '{LAKEHOUSE_NAME}': lakehouse['displayName']
    #     }
    #     return cls.update_part_in_create_request(create_request,
    #                                              'notebook-content.py',
    #                                              notebook_redirects,)

    # @classmethod
    # def get_create_report_request_from_folder(cls, item_folder, model_id):
    #     """generate create item request from folder"""
    #     create_request = cls.get_create_item_request_from_folder(item_folder)
    #     return cls.update_create_report_request_with_semantic_model(create_request,
    #                                                                 model_id)

    # @classmethod
    # def get_create_report_request_from_pbir_folder(cls, item_folder, model_id):
    #     """generate create item request from folder"""
    #     create_request = cls.get_create_item_request_from_folder(item_folder)
    #     return cls.update_create_pbir_report_request_with_semantic_model(create_request,
    #                                                                      model_id)
       

    @classmethod
    def update_part_in_create_request(cls, create_item_request, part_path, search_replace_text):
        """Update Item Definition Part"""
        item_definition = create_item_request['definition']
        item_part = next((part for part in item_definition['parts'] if part['path'] == part_path), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            item_part['payload'] = cls._search_and_replace_in_payload(item_part['payload'], search_replace_text)
            item_definition['parts'].append(item_part)
        
        return {
            'displayName': create_item_request['displayName'],
            'type': create_item_request['type'],
            'definition': item_definition
        }


    @classmethod
    def get_notebook_create_request(cls, workspace_id, lakehouse, display_name, py_file):
        """Create Item Definition for a Notebook"""
        py_content = cls.get_template_file(f"Notebooks//{py_file}")
        py_content = py_content.replace('{WORKSPACE_ID}', workspace_id) \
                             .replace('{LAKEHOUSE_ID}', lakehouse['id']) \
                             .replace('{LAKEHOUSE_NAME}', lakehouse['displayName'])

        item_part = cls._create_inline_base64_part('notebook-content.py', py_content)
        item_definition = {
            'parts': [ item_part ]
        }

        return {
            'displayName': display_name,
            'type': 'Notebook',
            'definition': item_definition
        }

    @classmethod
    def get_semantic_model_create_request(cls, display_name, bim_file):
        """Create semantic model create request from BIM file"""
        pbism_content = cls.get_template_file("SemanticModels//definition.pbism")
        bim_content  = cls.get_template_file(f'SemanticModels//{bim_file}')

        return {
            'displayName': display_name,
            'type': "SemanticModel",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbism', pbism_content),
                    cls._create_inline_base64_part('model.bim', bim_content)
                ]
            }
        }

    @classmethod
    def get_semantic_model_create_request_from_definition(cls, display_name, bim_definition):
        """Create semantic model create request from BIM file"""
        pbism_content = cls.get_template_file("SemanticModels//definition.pbism")

        return {
            'displayName': display_name,
            'type': "SemanticModel",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbism', pbism_content),
                    cls._create_inline_base64_part('model.bim', bim_definition)
                ]
            }
        }

    @classmethod
    def get_directlake_model_create_request(cls, display_name, bim_file, server, database):
        """Get Create Request for DirectLake Semantic Model"""

        pbism_content = cls.get_template_file("SemanticModels//definition.pbism")
        bim_template  = cls.get_template_file(f'SemanticModels//{bim_file}')

        bim_content = bim_template.replace('{SQL_ENDPOINT_SERVER}', server) \
                                  .replace('{SQL_ENDPOINT_DATABASE}', database)

        return {
            'displayName': display_name,
            'type': "SemanticModel",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbism', pbism_content),
                    cls._create_inline_base64_part('model.bim', bim_content)
                ]
            }
        }

    @classmethod
    def get_report_create_request(cls, semantic_model_id, display_name, report_json_file):
        """Get Create Request for Report using Report.json file"""

        pbir_content = \
            cls.get_template_file("Reports//definition.pbir").replace('{SEMANTIC_MODEL_ID}',
                                                                      semantic_model_id)

        report_json_content  = cls.get_template_file(f'Reports//{report_json_file}')
        theme_file_content = \
            cls.get_template_file(
                "Reports//StaticResources//SharedResources//BaseThemes//CY24SU02.json")
        return {
            'displayName': display_name,
            'type': "Report",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('definition.pbir', pbir_content),
                    cls._create_inline_base64_part('report.json', report_json_content),
                    cls._create_inline_base64_part(
                        'StaticResources/SharedResources/BaseThemes/CY24SU02.json', 
                        theme_file_content )
                ]
            }
        }

    @classmethod
    def update_report_definition_with_semantic_model_id(cls, item_definition, target_model_id):
        """Update Item Definition Part"""
        item_part = next((part for part in item_definition['parts'] if part['path'] == 'definition.pbir'), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            file_template = cls.get_template_file(r"Reports//definition.pbir")
            file_content = file_template.replace('{SEMANTIC_MODEL_ID}', target_model_id)
            item_part = cls._create_inline_base64_part('definition.pbir', file_content)
            item_definition['parts'].append(item_part)

        return item_definition

    @classmethod
    def update_create_report_request_with_semantic_model(cls, create_report_request, target_model_id):
        """Update Item Definition Part"""
        item_definition = create_report_request['definition']
        item_part = next((part for part in item_definition['parts'] if part['path'] == 'definition.pbir'), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            file_template = cls.get_template_file(r"Reports//definition.pbir")
            file_content = file_template.replace('{SEMANTIC_MODEL_ID}', target_model_id)
            item_part = cls._create_inline_base64_part('definition.pbir', file_content)
            item_definition['parts'].append(item_part)

        return {
            'displayName': create_report_request['displayName'],
            'type': "Report",
            'definition': item_definition
        }

    @classmethod
    def update_create_pbir_report_request_with_semantic_model(cls, create_report_request, target_model_id):
        """Update Item Definition Part"""
        item_definition = create_report_request['definition']
        item_part = next((part for part in item_definition['parts'] if part['path'] == 'definition.pbir'), None)
        if item_part is not None:
            item_definition['parts'].remove(item_part)
            file_template = cls.get_template_file(r"Reports//definition_pbir.pbir")
            file_content = file_template.replace('{SEMANTIC_MODEL_ID}', target_model_id)
            item_part = cls._create_inline_base64_part('definition.pbir', file_content)
            item_definition['parts'].append(item_part)

        return {
            'displayName': create_report_request['displayName'],
            'type': "Report",
            'definition': item_definition
        }

    @classmethod
    def get_data_pipeline_create_request(cls, display_name, pipeline_definition):
        """Get Create Request for Data Pipeline using pipeline-content.json file"""
        return {
            'displayName': display_name,
            'type': "DataPipeline",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('pipeline-content.json', pipeline_definition),
                ]
            }
        }

    @classmethod
    def get_eventhouse_create_request(cls, display_name, eventhouse_properties = None):                
        """Get Eventstream Create Request"""
        if eventhouse_properties is None:
            eventhouse_properties = cls.get_template_file(
                "Eventhouses//EventhouseProperties.json")
        return {
            'displayName': display_name,
            'type': "Eventhouse",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('EventhouseProperties.json', 
                                                   eventhouse_properties),
                ]
            }
        }

    @classmethod
    def get_kql_database_create_request(cls, display_name, eventhouse):
        """Get KQL Database Create Request"""

        database_schema = cls.get_template_file(
                "KqlDatabases//DatabaseSchema.kql")
        database_template = cls.get_template_file(
                "KqlDatabases//DatabaseProperties.json")
        database_properties = database_template.replace('{EVENTHOUSE_ID}', eventhouse['id'])
        
        return {
            'displayName': display_name,
            'type': "KQLDatabase",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('DatabaseProperties.json', database_properties),
                    cls._create_inline_base64_part('DatabaseSchema.kql', database_schema),
                ]
            }
        }

    @classmethod
    def get_eventstream_create_request(cls, display_name, workspace_id, eventhouse_id, kql_database):
        """Get Eventstream Create Request"""
        eventstream_properties = cls.get_template_file(
                "Eventstreams//eventstreamProperties.json")

        eventstream_template = cls.get_template_file(
                "Eventstreams//eventstream.json")
        
        eventstream_json = eventstream_template.replace('{WORKSPACE_ID}', workspace_id)\
                                               .replace('{KQL_DATABASE_ID}', kql_database['id'])\
                                               .replace('{KQL_DATABASE_NAME}', kql_database['displayName'])\
                                               .replace('{EVENTHOUSE_ID}', eventhouse_id)                             
        return {
            'displayName': display_name,
            'type': "Eventstream",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('eventstreamProperties.json', eventstream_properties),
                    cls._create_inline_base64_part('eventstream.json', eventstream_json),
                ]
            }
        }

    @classmethod
    def get_kql_dashboard_create_request(cls, display_name, workspace_id, kql_database, query_service_uri):
        """Get KQL Dashboard Create Request"""
        realtime_dashboard_template = cls.get_template_file("KqlDashboards//RealTimeDashboard.json")

        realtime_dashboard_json = realtime_dashboard_template.replace('{WORKSPACE_ID}', workspace_id)\
                                                             .replace('{KQL_DATABASE_ID}', kql_database['id'])\
                                                             .replace('{KQL_DATABASE_NAME}', kql_database['displayName'])\
                                                             .replace('{QUERY_SERVICE_URI}', query_service_uri)
        
        return {
            'displayName': display_name,
            'type': "KQLDashboard",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('RealTimeDashboard.json', realtime_dashboard_json),
                ]
            }
        }

    @classmethod
    def get_kql_queryset_create_request(cls, display_name, kql_database, query_service_uri, queryset_template):
        """Get KQL Queryset Create Request"""
        queryset_template = cls.get_template_file(f"KqlQuerysets//{queryset_template}")

        queryset_json = queryset_template.replace('{KQL_DATABASE_ID}', kql_database['id'])\
                                         .replace('{KQL_DATABASE_NAME}', kql_database['displayName'])\
                                         .replace('{QUERY_SERVICE_URI}', query_service_uri)
        
        return {
            'displayName': display_name,
            'type': "KQLQueryset",
            'definition': {
                'parts': [
                    cls._create_inline_base64_part('RealTimeQueryset.json', queryset_json),
                ]
            }
        }

    @classmethod
    def get_variable_library_create_request(cls, display_name, variable_library: VariableLibrary):
        """Get Create Request for Variable Library file"""

        variables_json = variable_library.get_variable_json()

        parts = [
            cls._create_inline_base64_part('variables.json', variables_json)
        ]

        for valueset in variable_library.valuesets:
            path = f"valueSets/{valueset.name}.json"
            content = variable_library.get_valueset_json(valueset.name)
            parts.append( cls._create_inline_base64_part(path, content ) )


        settings_json = cls.get_template_file("VariableLibraries//settings.json")
        parts.append( cls._create_inline_base64_part('settings.json', settings_json ) )

        return {
            'displayName': display_name,
            'type': "VariableLibrary",
            'definition': {
                'parts': parts
            }
        }

    @classmethod
    def get_update_variable_library_request(cls, variable_library: VariableLibrary):
        """Get Create Request for Variable Library file"""

        variables_json = variable_library.get_variable_json()

        parts = [
            cls._create_inline_base64_part('variables.json', variables_json)
        ]

        for valueset in variable_library.valuesets:
            path = f"valueSets/{valueset.name}.json"
            content = variable_library.get_valueset_json(valueset.name)
            parts.append( cls._create_inline_base64_part(path, content ) )


        settings_json = cls.get_template_file("VariableLibraries//settings.json")
        parts.append( cls._create_inline_base64_part('settings.json', settings_json ) )

        return {
            'definition': {
                'parts': parts
            }
        }

class DeploymentManager:
    """Deployment Manager"""

    @classmethod
    def update_imported_semantic_model_source(cls, workspace,
                                                semantic_model_name, 
                                                web_datasource_path):
        """Update Imported Sementic Model Source"""
        model = FabricRestApi.get_item_by_name(workspace['id'], semantic_model_name, 'SemanticModel')
        old_web_datasource_path = FabricRestApi.get_web_url_from_semantic_model(workspace['id'], model['id']) + '/'

        if web_datasource_path == old_web_datasource_path:
            AppLogger.log_substep(f"Verified web datasource path: [{web_datasource_path}]")
        else:
            old_model_definition = FabricRestApi.get_item_definition(workspace['id'], model)
    
            search_replace_terms = {
                old_web_datasource_path: web_datasource_path
            }

            model_definition = {
                'definition': ItemDefinitionFactory.update_item_definition_part(
                    old_model_definition['definition'],
                    'definition/expressions.tmdl',
                    search_replace_terms)
            }

            FabricRestApi.update_item_definition(workspace['id'], model, model_definition)

    @classmethod
    def update_directlake_semantic_model_source(cls,
                                                workspace_name,
                                                semantic_model_name,
                                                lakehouse_name):
        """Update DirectLake Sementic Model Source"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], lakehouse_name, 'Lakehouse')
        model = FabricRestApi.get_item_by_name(workspace['id'], semantic_model_name, 'SemanticModel')
        
        old_sql_endpoint =    FabricRestApi.get_sql_endpoint_from_semantic_model(
            workspace['id'],
            model['id']
        )

        new_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(
            workspace['id'],
            lakehouse)

        old_model_definition = FabricRestApi.get_item_definition(workspace['id'], model)

        search_replace_terms = {
            old_sql_endpoint['server']: new_sql_endpoint['server'],
            old_sql_endpoint['database']: new_sql_endpoint['database']
        }

        model_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part(
                old_model_definition['definition'],
                'definition/expressions.tmdl',
                search_replace_terms)
        }

        FabricRestApi.update_item_definition(workspace['id'], model, model_definition)

    @classmethod
    def update_datasource_path_in_notebook(cls,
        workspace_name,
        notebook_name,
        deployment_job):
        """Update datasource path in notebook"""
        
        redirects = {
            EnvironmentSettings.DEPLOYMENT_JOBS['dev']['web_datasource_path']: deployment_job['web_datasource_path'],
            EnvironmentSettings.DEPLOYMENT_JOBS['prod']['web_datasource_path']: deployment_job['web_datasource_path'],                    
        }
    
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        notebook = FabricRestApi.get_item_by_name(workspace['id'], notebook_name, 'Notebook')
        
        current_notebook_definition = FabricRestApi.get_item_definition(workspace['id'], notebook)

        notebook_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part(
                current_notebook_definition['definition'],
                'notebook-content.py',
                redirects)
        }

        FabricRestApi.update_item_definition(workspace['id'], notebook, notebook_definition)            

    @classmethod
    def update_source_lakehouse_in_notebook(cls,
                workspace_name,
                notebook_name,
                lakehouse_name):
        """Update datasource path in notebook"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        notebook = FabricRestApi.get_item_by_name(workspace['id'], notebook_name, 'Notebook')
        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], lakehouse_name, "Lakehouse")

        workspace_id = workspace['id']
        lakehouse_id = lakehouse['id']

        search_replace_terms = {
            r'("default_lakehouse"\s*:\s*)".*"': rf'\1"{lakehouse_id}"',
            r'("default_lakehouse_name"\s*:\s*)".*"': rf'\1"{lakehouse_name}"',
            r'("default_lakehouse_workspace_id"\s*:\s*)".*"': rf'\1"{workspace_id}"',
            r'("known_lakehouses"\s*:\s*)\[[\s\S]*?\]': rf'\1[{{"id": "{lakehouse_id}"}}]',
        }

        notebook_definition = FabricRestApi.get_item_definition(workspace['id'], notebook)

        notebook_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part_with_regex(
                notebook_definition['definition'],
                'notebook-content.py',
                search_replace_terms)
        }

        FabricRestApi.update_item_definition(workspace['id'], notebook, notebook_definition)

    @classmethod
    def create_and_bind_model_connection(cls, workspace_name):
        """Create and Bind Model Connections"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        model_name = 'Product Sales DirectLake Model'
        model = FabricRestApi.get_item_by_name(workspace['id'], model_name, 'SemanticModel')
        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'])

    @classmethod
    def apply_post_sync_fixes(cls, workspace_id, deployment_job, run_etl_jobs = False):

        """Apply Post Sync Fixes"""
        workspace = FabricRestApi.get_workspace_info(workspace_id)
        workspace_name = workspace['displayName']
        AppLogger.log_step(f"Applying post sync fixes to [{workspace['displayName']}]")
        
        workspace_items = FabricRestApi.list_workspace_items(workspace['id'])

        web_datasource_path = deployment_job['web_datasource_path']
        adls_container_name = deployment_job['adls_container_name']
        adls_container_path = deployment_job['adls_container_path']
        adls_server = deployment_job['adls_server']
        adls_path = f'/{adls_container_name}{adls_container_path}'

        lakehouses = list(filter(lambda item: item['type']=='Lakehouse', workspace_items))
        for lakehouse in lakehouses:
            shortcuts = FabricRestApi.list_shortcuts(workspace['id'], lakehouse['id'])
            for shortcut in shortcuts:
                connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                                adls_server,
                                adls_path,
                                workspace)

                shortcut_name = shortcut['name']
                shortcut_path = shortcut['path']
                shortcut_location = adls_server
                shortcut_subpath = adls_path
                AppLogger.log_substep(f'Creating shortcut to {shortcut_location}/{shortcut_subpath}')
                FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                                        lakehouse['id'],
                                                        shortcut_name,
                                                        shortcut_path,
                                                        shortcut_location,
                                                        shortcut_subpath,
                                                        connection['id'])

        notebooks = list(filter(lambda item: item['type']=='Notebook', workspace_items))
        for notebook in notebooks:
            # Apply fixes for [Create Lakehouse Tables.Notebook]
            if notebook['displayName'] in ['Create Lakehouse Tables',
                                           'Build 01 Silver Layer',
                                           'Build 02 Gold Layer',
                                           'Create Lakehouse Materialized Views',
                                           'Create Lakehouse Schemas',
                                           'Create Lakehouse Tables with VarLib']:
                # bind notebook to 'sales' lakehouse in same workspace
                AppLogger.log_substep(f"Updating notebook [{notebook['displayName']}]")
                cls.update_source_lakehouse_in_notebook(
                    workspace_name,
                    notebook['displayName'],
                    "sales")

                if notebook['displayName'] == 'Create Lakehouse Tables':
                    cls.update_datasource_path_in_notebook(
                        workspace_name,
                        notebook['displayName'],
                        deployment_job)

            if run_etl_jobs and 'Create' in notebook['displayName']:                
                FabricRestApi.run_notebook(workspace['id'], notebook)

        pipelines = list(filter(lambda item: item['type']=='DataPipeline', workspace_items))
        for pipeline in pipelines:
            if pipeline['displayName'] == 'Create Lakehouse Tables':
                pass
                # connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                # adls_server_path,
                # adls_path,
                # workspace)

                # create_pipeline_request = \
                #     ItemDefinitionFactory.get_create_item_request_from_folder(
                #         data_pipeline_folder)

                # pipeline_redirects = {
                #     '{WORKSPACE_ID}': workspace['id'],
                #     '{LAKEHOUSE_ID}': lakehouse['id'],
                #     '{CONNECTION_ID}': connection['id'],
                #     '{CONTAINER_NAME}': adls_container_name,
                #     '{CONTAINER_PATH}': adls_container_path,
                #     '{NOTEBOOK_ID_BUILD_SILVER}': notebook_ids[0],
                #     '{NOTEBOOK_ID_BUILD_GOLD}': notebook_ids[1]
                # }

                # create_pipeline_request = \
                #     ItemDefinitionFactory.update_part_in_create_request(
                #         create_pipeline_request,
                #         'pipeline-content.json',
                #         pipeline_redirects)

                # pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)

                # FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

        # sql_endpoints = list(filter(lambda item: item['type']=='SQLEndpoint', workspace_items))
        # for sql_endpoint in sql_endpoints:
        #     FabricRestApi.refresh_sql_endpoint_metadata(
                # workspace['id'],
                # sql_endpoint['id'])

        models = list(filter(lambda item: item['type']=='SemanticModel', workspace_items))
        for model in models:
            AppLogger.log_substep(f"Updating semantic model [{model['displayName']}]")
            # Apply fixes for [Product Sales Imported Model.SemanticModel]
            if model['displayName'] ==    'Product Sales Imported Model':
                # fix connection to imported models
                datasource_path =  deployment_job['web_datasource_path']

                DeploymentManager.update_imported_semantic_model_source(
                    workspace,
                    model['displayName'],
                    datasource_path)

                # FabricRestApi.create_and_bind_semantic_model_connecton(
                #     workspace,
                #     model['id'])

            # Apply fixes for [Product Sales DirectLake Model.SemanticModel]
            if model['displayName'] ==    'Product Sales DirectLake Model':
                # fix connection to lakehouse SQL endpoint
                target_lakehouse_name = 'sales'
                DeploymentManager.update_directlake_semantic_model_source(
                    workspace_name, 
                    model['displayName'],
                    target_lakehouse_name)

                # FabricRestApi.create_and_bind_semantic_model_connecton(
                #     workspace,
                #     model['id'])

    @classmethod
    def apply_post_deploy_fixes(cls,
                                workspace_id,
                                deployment_job,
                                run_etl_jobs = True):
        
        """Apply Post Deploy Fixes"""
        workspace = FabricRestApi.get_workspace_info(workspace_id)
        workspace_name = workspace['displayName']                 
        AppLogger.log_step(f"Applying post deploy fixes to [{workspace_name}]")
        workspace_items = FabricRestApi.list_workspace_items(workspace['id'])
        
        variable_libraries = list(filter(lambda item: item['type']=='VariableLibrary', workspace_items))
        for variable_library in variable_libraries:
            FabricRestApi.set_active_valueset_for_variable_library(
                workspace['id'],
                variable_library,
                deployment_job['name']
            )

        lakehouses = list(filter(lambda item: item['type']=='Lakehouse', workspace_items))
        for lakehouse in lakehouses:
            shortcuts = FabricRestApi.list_shortcuts(workspace['id'], lakehouse['id'])
            for shortcut in shortcuts:
                if (shortcut['target']['type'] == 'AdlsGen2') and (shortcut['name'] == 'sales-data'):
                    FabricRestApi.reset_adls_gen2_shortcut(workspace['id'], lakehouse['id'], shortcut)

        notebooks = list(filter(lambda item: item['type']=='Notebook', workspace_items))
        for notebook in notebooks:  
            notebooks_for_sales_lakehouse = [
                'Create Lakehouse Tables', 
                'Create 01 Silver Layer', 
                'Create 02 Gold Layer', 
                'Build 01 Silver Layer', 
                'Build 02 Gold Layer']
            
            if notebook['displayName'] in notebooks_for_sales_lakehouse:
                cls.update_source_lakehouse_in_notebook(
                    workspace_name,
                    notebook['displayName'],
                    "sales")

            if notebook['displayName'] == 'Create Lakehouse Tables':
                cls.update_datasource_path_in_notebook(
                    workspace_name,
                    notebook['displayName'],
                    deployment_job)
                
            if run_etl_jobs and 'Create' in notebook['displayName']:                
                FabricRestApi.run_notebook(workspace['id'], notebook)

        pipelines  = list(filter(lambda item: item['type']=='DataPipeline', workspace_items))
        for pipeline in pipelines:
            # run pipelines if required                
            if run_etl_jobs and 'Create' in pipeline['displayName']:
                FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

        sql_endpoints = list(filter(lambda item: item['type']=='SQLEndpoint', workspace_items))
        for sql_endpoint in sql_endpoints:
            FabricRestApi.refresh_sql_endpoint_metadata(
                workspace['id'],
                sql_endpoint['id'])

        models = list(filter(lambda item: item['type']=='SemanticModel', workspace_items))
        for model in models:

            # Apply fixes for [Product Sales Imported Model.SemanticModel]
            if model['displayName'] ==    'Product Sales Imported Model':
                # fix connection to imported models
                datasource_path =    \
                    deployment_job['web_datasource_path']

                DeploymentManager.update_imported_semantic_model_source(
                    workspace,
                    model['displayName'],
                    datasource_path)

                FabricRestApi.create_and_bind_semantic_model_connecton(
                    workspace,
                    model['id'])

            # Apply fixes for [Product Sales DirectLake Model.SemanticModel]
            if model['displayName'] ==    'Product Sales DirectLake Model':
                # fix connection to lakehouse SQL endpoint
                target_lakehouse_name = 'sales'
                DeploymentManager.update_directlake_semantic_model_source(
                    workspace_name, 
                    model['displayName'],
                    target_lakehouse_name)
                
                target_lakehouse = FabricRestApi.get_item_by_name(
                    workspace['id'], 
                    target_lakehouse_name, 
                    "Lakehouse")
                
                FabricRestApi.create_and_bind_semantic_model_connecton(
                    workspace,
                    model['id'],
                    target_lakehouse)

    # @classmethod
    # def apply_post_deploy_fixes(cls,
    #                             workspace_id,
    #                             deployment_job,
    #                             run_etl_jobs = False):
        
    #     """Apply Post Deploy Fixes"""                    
    #     workspace = FabricRestApi.get_workspace_info(workspace_id)
    #     workspace_name = workspace['displayName']
    #     AppLogger.log_step(f"Applying post deploy fixes to [{workspace_name}]")
    #     workspace_items = FabricRestApi.list_workspace_items(workspace['id'])

    #     web_datasource_path = deployment_job['web_datasource_path']
    #     adls_container_name = deployment_job['adls_container_name']
    #     adls_container_path = deployment_job['adls_container_path']
    #     adls_server = deployment_job['adls_server']
    #     adls_path = f'/{adls_container_name}{adls_container_path}'

    #     lakehouses = list(filter(lambda item: item['type']=='Lakehouse', workspace_items))
    #     for lakehouse in lakehouses:
    #         shortcuts = FabricRestApi.list_shortcuts(workspace['id'], lakehouse['id'])
    #         for shortcut in shortcuts:
    #             connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
    #                             adls_server,
    #                             adls_path,
    #                             workspace)

    #             shortcut_name = shortcut['name']
    #             shortcut_path = shortcut['path']
    #             shortcut_location = adls_server
    #             shortcut_subpath = adls_path

    #             FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
    #                                                     lakehouse['id'],
    #                                                     shortcut_name,
    #                                                     shortcut_path,
    #                                                     shortcut_location,
    #                                                     shortcut_subpath,
    #                                                     connection['id'])

    #     notebooks = list(filter(lambda item: item['type']=='Notebook', workspace_items))
    #     for notebook in notebooks:
    #         # Apply fixes for [Create Lakehouse Tables.Notebook]
    #         if notebook['displayName'] == 'Create Lakehouse Tables':
    #             cls.update_source_lakehouse_in_notebook(
    #                 workspace_name,
    #                 notebook['displayName'],
    #                 "sales")

    #             cls.update_datasource_path_in_notebook(
    #                 workspace_name,
    #                 notebook['displayName'],
    #                 deployment_job)
                
    #         if run_etl_jobs and 'Create' in notebook['displayName']:                
    #             FabricRestApi.run_notebook(workspace['id'], notebook)

    #     sql_endpoints =    list(filter(lambda item: item['type']=='SQLEndpoint', workspace_items))
    #     for sql_endpoint in sql_endpoints:
    #         FabricRestApi.refresh_sql_endpoint_metadata(
    #             workspace['id'],
    #             sql_endpoint['id'])

    #     models = list(filter(lambda item: item['type']=='SemanticModel', workspace_items))
    #     for model in models:

    #         # Apply fixes for [Product Sales Imported Model.SemanticModel]
    #         if model['displayName'] ==    'Product Sales Imported Model':
    #             # fix connection to imported models
    #             datasource_path = web_datasource_path

    #             DeploymentManager.update_imported_semantic_model_source(
    #                 workspace,
    #                 model['displayName'],
    #                 datasource_path)

    #             FabricRestApi.create_and_bind_semantic_model_connecton(
    #                 workspace,
    #                 model['id'])

    #         # Apply fixes for [Product Sales DirectLake Model.SemanticModel]
    #         if model['displayName'] ==    'Product Sales DirectLake Model':
    #             # fix connection to lakehouse SQL endpoint
    #             target_lakehouse_name = 'sales'
    #             DeploymentManager.update_directlake_semantic_model_source(
    #                 workspace_name, 
    #                 model['displayName'],
    #                 target_lakehouse_name)

    #             FabricRestApi.create_and_bind_semantic_model_connecton(
    #                 workspace,
    #                 model['id'])

    @classmethod
    def run_data_pipeline(cls, workspace_name, data_pipeline_name):
        """Run data pipeline"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        data_pipeline_name = 'Create Lakehouse Tables'
        data_pipeline = FabricRestApi.get_item_by_name(
            workspace['id'],
            data_pipeline_name,
            'DataPipeline')

        FabricRestApi.run_data_pipeline(workspace['id'], data_pipeline)

    @classmethod
    def get_sql_endpoint_info_by_name(cls, workspace_name, lakehouse_name):
        """Get SQL Endpoint"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], 
                                                     lakehouse_name, 'Lakehouse')
        
        return FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)
 