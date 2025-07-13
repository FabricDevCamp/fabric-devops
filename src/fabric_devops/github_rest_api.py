"""Module to manage calls to Fabric REST APIs"""

import time
import base64
import os
from json.decoder import JSONDecodeError

from nacl import encoding, public
import requests

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings

class GitHubRestApi:
    """Wrapper class for calling GitHub REST APIs"""

    ACCESS_TOKEN = EnvironmentSettings.PERSONAL_ACCESS_TOKEN_GITHUB

    GITHUB_ORGANIZATION = 'fabricdevcampdemos'
    GITHUB_OWNER = 'TedAtDevCamp'
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
            f'Error executing POST request: {response.status_code} - {response.text}')

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
        endpoint = f"orgs/{cls.GITHUB_ORGANIZATION}/repos"
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
    def create_repository(cls, repo_name, branches = None, private = False, add_secrets = False):
        """Create GitHub Repository"""
        AppLogger.log_step(f"Creating new GitHub repo [{repo_name}]")
        repo = cls.get_github_repository_by_name(repo_name)
        if repo is not None:
            AppLogger.log_substep("Deleting existing repo with same name")
            cls.delete_github_repository(repo_name)

        endpoint = f"/orgs/{cls.GITHUB_ORGANIZATION}/repos"
        body = {
            'name': repo_name,
            'private': private,
            'auto_init': True
        }
        repo = cls._execute_post_request(endpoint, body)
        AppLogger.log_substep("Repo created successfully")

        cls.create_workspace_readme(repo_name)

        if branches is not None:
            for branch in branches:
                cls.create_branch(repo_name, branch)

        if add_secrets:
            cls.add_repository_secrets(repo_name)

        return repo

    @classmethod
    def delete_github_repository(cls, repo_name):
        """Delete GitHub Repository"""
        endpoint = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}"
        return cls._execute_delete_request(endpoint)

    @classmethod
    def list_branches(cls, repo_name):
        """Create GitHub Repository Branch"""
        endpoint = f"/orgs/{cls.GITHUB_ORGANIZATION}/repos/{repo_name}/branches"
        repos = cls._execute_get_request(endpoint)
        return repos

    @classmethod
    def create_branch(cls, repo_name, branch_name):
        """Create GitHub Repository Branch"""
        AppLogger.log_substep(f"Creating branch [{branch_name}]")

        endpoint_branchces = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/git/refs/heads"
        branches = cls._execute_get_request(endpoint_branchces)
        sha = branches[-1]['object']['sha']

        endpoint_refs = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/git/refs"

        body = {
            'ref': f'refs/heads/{branch_name}',
            "sha": sha
        }

        return cls._execute_post_request(endpoint_refs, body)

    @classmethod
    def create_pull_request(
        cls, 
        repo_name, 
        source_branch_name, 
        target_branch_name, 
        commit_title, 
        commit_comment):
        """Create GitHub Repository Branch"""
        AppLogger.log_substep(f"Creating pull request for branch [{source_branch_name}]")

        endpoint_pull_requests = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/pulls"

        body = {
            'owner': cls.GITHUB_ORGANIZATION,
            'repo': repo_name,
            'title': commit_title,
            'body': commit_comment,
            'head': (cls.GITHUB_ORGANIZATION + ":" + source_branch_name),
            'base': target_branch_name
        }

        return cls._execute_post_request(
            endpoint_pull_requests,
            body,
            content_type='application/vnd.github.raw+json')

    @classmethod
    def merge_pull_request(cls, repo_name, pull_number, commit_title, commit_comment):
        """Create GitHub Repository Branch"""
        AppLogger.log_substep(f"Merge pull request [{commit_title}]")

        endpoint_pull_request = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/pulls/{pull_number}"
        pull_request = cls._execute_get_request(endpoint_pull_request)

        pull_request_mergable = pull_request['mergeable']

        while pull_request_mergable is not True:
            time.sleep(2)
            pull_request = cls._execute_get_request(endpoint_pull_request)
            pull_request_mergable = pull_request['mergeable']

        endpoint_pull_request_merge = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/pulls/{pull_number}/merge"

        body = {
            'owner': cls.GITHUB_ORGANIZATION,
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
        source_branch_name,
        target_branch_name,
        commit_title,
        commit_comment):
        """Create and Merge Pull Request"""
        AppLogger.log_step(f"Creating and merging pull request for branch [{source_branch_name}]")

        pull_request = cls.create_pull_request(
            repo_name, 
            source_branch_name,
            target_branch_name,
            commit_title,
            commit_comment
        )
        
        pull_request_number = pull_request['number']
        cls.merge_pull_request(repo_name, pull_request_number, commit_title, commit_comment)

    @classmethod
    def create_workspace_readme(cls, repo_name):
        """Create Content"""
        content = f"## ReadMe file for {repo_name}"
        base64_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')

        endpoint = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/contents/workspace/readme.md"

        body = {
            "message":"default message",
            "content": base64_content
        }
        
        cls._execute_put_request(endpoint, body)

    @classmethod
    def write_file_to_repo(cls, repo_name, branch, file_path, file_content, comment = 'default message'):
        """Create Content"""        
        base64_content = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')
        endpoint = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/contents/{file_path}"
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
        endpoint = f"repos/{cls.GITHUB_ORGANIZATION}/{repo_name}"
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
        folder_path = f".//templates//WorkflowActions//{folder_path}//"
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
        endpoint = f"/orgs/{cls.GITHUB_ORGANIZATION}/actions/secrets/public-key"
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

        endpoint = f"/orgs/{cls.GITHUB_ORGANIZATION}/actions/secrets/{secret_name}"
        body = {
            'owner': cls.GITHUB_ORGANIZATION,
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
        endpoint = f"/repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/actions/secrets/public-key"
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

        endpoint = f"/repos/{cls.GITHUB_ORGANIZATION}/{repo_name}/actions/secrets/{secret_name}"
        body = {
            'owner': cls.GITHUB_ORGANIZATION,
            'repo': repo_name,
            'secret_name': secret_name,
            'encrypted_value': base64_encrypted,
            'key_id': public_key_id
        }

        cls._execute_put_request(endpoint, body, 'application/vnd.github+json')

    @classmethod
    def add_repository_secrets(cls, repo_name):
        """Add respository secrets"""
        cls.create_repository_secret(repo_name, 'FABRIC_CLIENT_ID', EnvironmentSettings.FABRIC_CLIENT_ID)
        cls.create_repository_secret(repo_name, 'FABRIC_CLIENT_SECRET', EnvironmentSettings.FABRIC_CLIENT_SECRET)
        cls.create_repository_secret(repo_name, 'FABRIC_TENANT_ID', EnvironmentSettings.FABRIC_TENANT_ID)
        cls.create_repository_secret(repo_name, 'ADMIN_USER_ID', EnvironmentSettings.ADMIN_USER_ID)
        cls.create_repository_secret(repo_name, 'FABRIC_CAPACITY_ID', EnvironmentSettings.FABRIC_CAPACITY_ID)
        cls.create_repository_secret(repo_name, 'PERSONAL_ACCESS_TOKEN_GITHUB', EnvironmentSettings.PERSONAL_ACCESS_TOKEN_GITHUB)
        cls.create_repository_secret(repo_name, 'SERVICE_PRINCIPAL_OBJECT_ID', EnvironmentSettings.SERVICE_PRINCIPAL_OBJECT_ID)
