"""Module to manage calls to GitHub REST APIs"""

import time
import base64
import os
import json
from json.decoder import JSONDecodeError

from nacl import encoding, public
import requests

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings
from .item_definition_factory import ItemDefinitionFactory

class GitHubRestApi:
    """Wrapper class for calling GitHub REST APIs"""

    ACCESS_TOKEN = EnvironmentSettings.PERSONAL_ACCESS_TOKEN_GITHUB

    ORGANIZATION_GITHUB = EnvironmentSettings.ORGANIZATION_GITHUB
    BASE_URL = 'https://api.github.com/'

#region Low-level details about authentication and HTTP requests and responses

    @classmethod
    def _execute_get_request(cls, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = cls.BASE_URL + endpoint
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'token {cls.ACCESS_TOKEN}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404: # NOT FOUND error
            return None
        else:
            AppLogger.log_error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request(cls, endpoint, post_body='', content_type = 'application/json'):

        """Execute POST request with support for Long-running Operations (LRO)"""
        rest_url = cls.BASE_URL + endpoint
        request_headers = {'Content-Type': content_type,
                           'Authorization': f'token {cls.ACCESS_TOKEN}'}           
        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code == 202:
            AppLogger.log_step("Got back 202 ACCEPTED from POST Request")
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
        else:
            AppLogger.log_error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_put_request(cls, endpoint, post_body='',  content_type = 'application/json'):
        """Execute PUT request """
        rest_url = cls.BASE_URL + endpoint
        request_headers = {
            'Content-Type': content_type,
            'Authorization': f'token {cls.ACCESS_TOKEN}'
        }           
        response = requests.put(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code == 202:
            AppLogger.log_step("Got back 202 ACCEPTED from PUT  Request")

        AppLogger.log_error(
            f'Error executing PUT request: {response.status_code} - {response.text}')

    @classmethod
    def _execute_put_request_with_file(cls, endpoint, post_body=''):
        """Execute PUT request """
        rest_url = cls.BASE_URL + endpoint
        request_headers = {'Content-Type':'application/vnd.github+json',
                           'Authorization': f'token {cls.ACCESS_TOKEN}'}           
        response = requests.put(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        AppLogger.log_error(
            f'Error executing Put request: {response.status_code} - {response.text}')

    @classmethod
    def _execute_patch_request(cls, endpoint, post_body):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = cls.BASE_URL + endpoint
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'token {cls.ACCESS_TOKEN}'}
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
    def _execute_delete_request(cls, endpoint):
        """Execute DELETE Request on Fabric REST API Endpoint"""
        rest_url = cls.BASE_URL + endpoint
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'token {cls.ACCESS_TOKEN}'}

        response = requests.delete(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code in { 200, 204 }:
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

#endregion

    @classmethod
    def get_github_user(cls):
        """Get GitHub User"""
        endpoint = "user"
        return cls._execute_get_request(endpoint)

    @classmethod
    def get_github_repositories(cls):
        """Get GitHub Repositories"""
        endpoint = f"orgs/{cls.ORGANIZATION_GITHUB}/repos"
        return cls._execute_get_request(endpoint)

    @classmethod
    def display_github_repos(cls):
        """Display GitHub Repositories"""
        AppLogger.log_step('Repos')
        repos = cls.get_github_repositories()
        for repo in repos:
            AppLogger.log_substep(f"{repo['name']} [{repo['id']}]")

    @classmethod
    def get_github_repository_by_name(cls, repo_name):
        """Get GitHub Repository by Name"""
        repos = cls.get_github_repositories()
        for repo in repos:
            if repo['name'] == repo_name:
                return repo
        return None

    @classmethod
    def create_repository(cls, repo_name, workspace = None, private = False):
        """Create GitHub Repository"""
        AppLogger.log_step(f"Creating new GitHub repository [{repo_name}]")
        repo = cls.get_github_repository_by_name(repo_name)
        if repo is not None:
            AppLogger.log_substep("Deleting existing repo with same name")
            cls.delete_github_repository(repo_name)

        endpoint = f"/orgs/{cls.ORGANIZATION_GITHUB}/repos"
        body = {
            'name': repo_name,
            'private': private,
            'auto_init': True
        }
        
        cls._execute_post_request(endpoint, body)
             
        AppLogger.log_substep(f"Repo [{repo_name}] created successfully")
        
        if workspace is None:
            root_folder_readme_content = ItemDefinitionFactory.get_template_file(
                'GitHubRepoTemplates/GitHubReadMe.md')
        else:
            root_folder_readme_content = ItemDefinitionFactory.get_template_file(
                'GitHubRepoTemplates/GitHubReadMeWithWorkspace.md')

            root_folder_readme_content = root_folder_readme_content.replace(
                '{WORKSPACE_ID}', 
                workspace.id)

            root_folder_readme_content = root_folder_readme_content.replace(
                '{WORKSPACE_NAME}', 
                workspace.display_name)

        cls.overwrite_repo_readme_file(repo_name, root_folder_readme_content)

        # create readme file in /workspace folder as placeholder
        cls.create_workspace_folder_readme(repo_name, 'main')

        return repo

    @classmethod
    def delete_github_repository(cls, repo_name):
        """Delete GitHub Repository"""
        endpoint = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}"
        return cls._execute_delete_request(endpoint)

    @classmethod
    def list_branches(cls, repo_name):
        """Create GitHub Repository Branch"""
        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/branches"
        repos = cls._execute_get_request(endpoint)
        return repos

    @classmethod
    def get_branch_by_name(cls, repo_name, branch_name):
        """Create GitHub Repository Branch"""
        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/branches/{branch_name}"
        repos = cls._execute_get_request(endpoint)
        return repos

    @classmethod
    def get_sha_for_branch(cls, repo_name, branch_name):
        """Create GitHub Repository Branch"""
        branch = cls.get_branch_by_name(repo_name, branch_name)
        return branch['commit']['sha']

    @classmethod
    def overwrite_repo_readme_file(cls, repo_name, contents):
        """Get a specific file from a GitHub repository branch"""
        
        # get readme file sha
        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/readme"
        
        readme_file = cls._execute_get_request(endpoint)
        
        while readme_file is None:
            AppLogger.log_substep("README.md file not found, retrying in 5 seconds...")
            time.sleep(5)
            readme_file = cls._execute_get_request(endpoint)
        
        readme_file_sha = readme_file['sha']
        
        # overwrite readme file with new content
        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/contents/README.md"
        base64_content = base64.b64encode(contents.encode('utf-8')).decode('utf-8')
        body = {
            'message': 'Overwrite README.md',
            'content': base64_content,
            'sha': readme_file_sha
        }
        cls._execute_put_request(endpoint, body)

    @classmethod
    def compare_branches(cls, repo_name, source_branch, target_branch):
        """Create GitHub Repository Branch"""
        endpoint = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/compare/{target_branch}...{source_branch}"
        result = cls._execute_get_request(endpoint)
        return result

    @classmethod
    def create_branch(cls, repo_name, branch_name, source_branch = 'main'):
        """Create GitHub Repository Branch"""
        AppLogger.log_substep(f"Creating branch [{branch_name}] from [{source_branch}]")
        
        sha = cls.get_sha_for_branch(repo_name, source_branch)

        endpoint_refs = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/git/refs"

        body = {
            'ref': f'refs/heads/{branch_name}',
            "sha": sha
        }

        return cls._execute_post_request(endpoint_refs, body)

    @classmethod
    def create_pull_request(
        cls, 
        repo_name, 
        source_branch, 
        target_branch, 
        commit_title, 
        commit_comment):
        """Create GitHub Repository Branch"""
        AppLogger.log_substep(f"Creating pull request for branch [{source_branch}]")

        endpoint_pull_requests = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/pulls"

        body = {
            'owner': cls.ORGANIZATION_GITHUB,
            'repo': repo_name,
            'title': commit_title,
            'body': commit_comment,
            'head': (cls.ORGANIZATION_GITHUB + ":" + source_branch),
            'base': target_branch
        }

        return cls._execute_post_request(
            endpoint_pull_requests,
            body,
            content_type='application/vnd.github.raw+json')

    @classmethod
    def merge_pull_request(cls, repo_name, pull_number, commit_title, commit_comment):
        """Create GitHub Repository Branch"""
        AppLogger.log_substep(f"Merge pull request [{commit_title}]")

        endpoint_pull_request = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/pulls/{pull_number}"
        pull_request = cls._execute_get_request(endpoint_pull_request)

        pull_request_mergable = pull_request['mergeable']

        while pull_request_mergable is not True:
            time.sleep(2)
            pull_request = cls._execute_get_request(endpoint_pull_request)
            pull_request_mergable = pull_request['mergeable']

        endpoint_pull_request_merge = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/pulls/{pull_number}/merge"

        body = {
            'owner': cls.ORGANIZATION_GITHUB,
            'repo': repo_name, 
            'pull_number': str(pull_number),
            'commit_title': commit_title,
            'commit_message': commit_comment,
        }

        return cls._execute_put_request(
            endpoint_pull_request_merge,
            body,
            content_type='application/vnd.github+json')

    @classmethod
    def create_and_merge_pull_request(
        cls,
        repo_name,
        source_branch,
        target_branch,
        commit_title,
        commit_comment):
        """Create and Merge Pull Request"""
        AppLogger.log_step(f"Merging changes from [{source_branch}] to [{target_branch}]")

        comparison = GitHubRestApi.compare_branches(repo_name, source_branch, target_branch)

        comparison_has_no_commits = len(comparison['commits']) == 0
        
        if comparison_has_no_commits:
            AppLogger.log_substep("No need to create pull request because the two branches are already in sync")
            return
              
        pull_request = cls.create_pull_request(
            repo_name,
            source_branch,
            target_branch,
            commit_title,
            commit_comment
        )
        
        pull_request_number = pull_request['number']
        cls.merge_pull_request(repo_name, pull_request_number, commit_title, commit_comment)

    @classmethod
    def create_workspace_folder_readme(cls, repo_name, branch_name):
        """Create Content"""
        
        content = ItemDefinitionFactory.get_template_file(
            'GitHubRepoTemplates/GitHubReadMeForWorkspaceFolder.md'
        )
        
        base64_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')

        endpoint = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/contents/workspace/README.md"

        body = {
            "message":"Adding ReadMe.md file as placehilder in /workspace folder",
            "content": base64_content,
            'branch': branch_name
        }
        
        cls._execute_put_request(endpoint, body)

    @classmethod
    def write_file_to_repo(cls, repo_name, branch, file_path, file_content, comment = None):
        """Create Content"""        
        if comment is None:
            comment = f'Writing file {file_path}'
        
        base64_content = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')
        endpoint = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/contents/{file_path}"
        body = {
            "message": comment,
            "content": base64_content,
            "branch": branch
        }
        
        cls._execute_put_request_with_file(endpoint, body)

    @classmethod
    def set_default_branch(cls, repo_name, default_branch_name):
        """Create GitHub Repository Branch"""
        AppLogger.log_substep(f"Setting [{default_branch_name}] branch as default branch")
        endpoint = f"repos/{cls.ORGANIZATION_GITHUB}/{repo_name}"
        body = {
            'name': repo_name,
            'default_branch': default_branch_name
        }
        cls._execute_patch_request(endpoint, body)

    @classmethod
    def _get_part_path(cls, item_folder_path, file_path):
        """get path for item definition part"""
        offset = file_path.find(item_folder_path) + len(item_folder_path)
        return file_path[offset:].replace('\\', '/')

    @classmethod
    def copy_files_from_folder_to_repo(cls, repo_name, branch, folder_path):
        """Copy files to repo"""
        folder_path = f".//templates//ProjectWorkflowFiles//{folder_path}//"
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                file = open(file_path,'r', encoding="utf-8")
                file_content = file.read()
                relative_file_path = file_path.replace(folder_path, '')\
                                              .replace('\\', '/')
                cls.write_file_to_repo(repo_name, branch, relative_file_path, file_content)

    @classmethod
    def _get_org_public_key(cls):
        """Create GitHub Repository Branch"""
        endpoint = f"/orgs/{cls.ORGANIZATION_GITHUB}/actions/secrets/public-key"
        return cls._execute_get_request(endpoint)

    @classmethod
    def create_organization_secret(cls, repo_name, secret_name, secret_value):
        """Create repository secret"""

        public_key_result = cls._get_org_public_key()
        public_key_id = public_key_result['key_id']
        public_key_value = public_key_result['key']        
        public_key = public.PublicKey(public_key_value.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key)
        encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
        base64_encrypted = base64.b64encode(encrypted).decode("utf-8")

        endpoint = f"/orgs/{cls.ORGANIZATION_GITHUB}/actions/secrets/{secret_name}"
        body = {
            'owner': cls.ORGANIZATION_GITHUB,
            'repo': repo_name,
            'secret_name': secret_name,
            'encrypted_value': base64_encrypted,
            'key_id': public_key_id,
            'visibility': 'all'
        }

        cls._execute_put_request(endpoint, body, 'application/vnd.github+json')

    @classmethod
    def _get_repo_public_key(cls, repo_name):
        """Create GitHub Repository Branch"""
        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/actions/secrets/public-key"
        return cls._execute_get_request(endpoint)

    @classmethod
    def create_repository_secret(cls, repo_name, secret_name, secret_value):
        """Create repository secret"""

        public_key_result = cls._get_repo_public_key(repo_name)
        public_key_id = public_key_result['key_id']
        public_key_value = public_key_result['key']        
        public_key = public.PublicKey(public_key_value.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key)
        encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
        base64_encrypted = base64.b64encode(encrypted).decode("utf-8")

        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/actions/secrets/{secret_name}"
        body = {
            'owner': cls.ORGANIZATION_GITHUB,
            'repo': repo_name,
            'secret_name': secret_name,
            'encrypted_value': base64_encrypted,
            'key_id': public_key_id
        }

        cls._execute_put_request(endpoint, body, 'application/vnd.github+json')

    @classmethod
    def create_repository_variable(cls, repo_name, variable_name, variable_value):
        """Create repository variable"""

        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/actions/variables"
        body = {
            'owner': cls.ORGANIZATION_GITHUB,
            'repo': repo_name,
            'name': variable_name,
            'value': variable_value,
        }

        cls._execute_post_request(endpoint, body, 'application/vnd.github+json')

    @classmethod
    def create_variables_for_github_project(
        cls, 
        repo_name, 
        dev_workspace_id, 
        test_workspace_id,
        prod_workspace_id):
        """Add Variables to GitHub repo"""

        AppLogger.log_step(f"Creating variables in repo [{repo_name}]")        
        
        cls.create_repository_secret(repo_name, 'AZURE_CLIENT_ID', EnvironmentSettings.AZURE_CLIENT_ID)
        cls.create_repository_secret(repo_name, 'AZURE_CLIENT_SECRET', EnvironmentSettings.AZURE_CLIENT_SECRET)
        cls.create_repository_secret(repo_name, 'AZURE_TENANT_ID', EnvironmentSettings.AZURE_TENANT_ID)
        cls.create_repository_secret(repo_name, 'PERSONAL_ACCESS_TOKEN_GITHUB', EnvironmentSettings.PERSONAL_ACCESS_TOKEN_GITHUB)
        cls.create_repository_variable(repo_name, 'ORGANIZATION_GITHUB', EnvironmentSettings.ORGANIZATION_GITHUB)
        cls.create_repository_variable(repo_name, 'ADMIN_USER_ID', EnvironmentSettings.ADMIN_USER_ID)
        cls.create_repository_variable(repo_name, 'FABRIC_CAPACITY_ID', EnvironmentSettings.FABRIC_CAPACITY_ID)
        cls.create_repository_variable(repo_name, 'WORKSPACE_ID_DEV', dev_workspace_id)
        cls.create_repository_variable(repo_name, 'WORKSPACE_ID_TEST', test_workspace_id)
        cls.create_repository_variable(repo_name, 'WORKSPACE_ID_PROD', prod_workspace_id)

    @classmethod
    def run_workflow(cls, repo_name, workflow_file_name, branch):
        """Run GitHub Workflow"""
        
        AppLogger.log_step(f"Running Workflow [{workflow_file_name}] on branch [{branch}]") 
        
        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/actions/workflows/{workflow_file_name}/dispatches"
        body = { 
            "ref":branch,
            'return_run_details': True
        }
        
        reponse = cls._execute_post_request(endpoint, body)
        workflow_run_id = reponse['workflow_run_id']
        workflow_run = cls.get_workflow_run(repo_name, workflow_run_id)
        
        AppLogger.log_substep(f"Workflow run status: {workflow_run['status']}")
        
        while workflow_run['status'] != 'completed':
            time.sleep(20)         
            workflow_run = cls.get_workflow_run(repo_name, workflow_run_id)
            AppLogger.log_substep(f"Workflow run status: {workflow_run['status']}")

        AppLogger.log_substep(f"Workflow run conclusion: {workflow_run['conclusion']}")
        
    @classmethod
    def get_workflow_run(cls, repo_name, run_id):
        """Get GitHub Workflow Run Status"""
        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/actions/runs/{run_id}"
        response = cls._execute_get_request(endpoint)
        return response


    @classmethod
    def create_environment(cls, repo_name, environment_name, add_reviewers=False):
        """Create or update environment with protection rules"""

        endpoint = f"/repos/{cls.ORGANIZATION_GITHUB}/{repo_name}/environments/{environment_name}"
        
        body = {
            'wait_timer': 240
        }
        
        # Only set prevent_self_review if required_reviewers are provided
        if add_reviewers:
            body['prevent_self_review'] = False
            body['reviewers'] = [
                {
                    'type': 'User',
                    'id': GitHubRestApi.get_github_user()['id']
                }
            ]

        cls._execute_put_request(endpoint, body, 'application/vnd.github+json')

