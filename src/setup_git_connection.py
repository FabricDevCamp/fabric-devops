"""Setup GIT Connection"""

import os

from fabric_devops import DeploymentManager, AppLogger, GitHubRestApi, FabricRestApi

os.system('cls')

def display_github_repos():
    """Display GitHub Repositories"""
    AppLogger.log_step('Repos')
    repos = GitHubRestApi.get_github_repositories()
    for repo in repos:
        AppLogger.log_substep(f"{repo['name']} [{repo['id']}]")

def setup_workspace_with_github_repo(target_workspace):
    """Setup Workspace with GIT Connection"""

    workspace = FabricRestApi.get_workspace_by_name(target_workspace)

    if workspace is None:
        workspace = DeploymentManager.deploy_powerbi_solution(target_workspace)
    
    GitHubRestApi.create_repository(target_workspace)
    GitHubRestApi.copy_files_from_folder_to_repo(WORKSPACE_NAME, 'Hello')
    GitHubRestApi.create_branch(WORKSPACE_NAME, 'test')
    GitHubRestApi.create_branch(WORKSPACE_NAME, 'dev')
    GitHubRestApi.set_default_branch(WORKSPACE_NAME, 'dev')

    FabricRestApi.connect_workspace_to_github_repository(workspace, target_workspace, 'dev')

def get_workspace_git_status(workspace_name):
    """Get Workspace GIT status"""
    workspace = FabricRestApi.get_workspace_by_name(workspace_name)

    my_creds = FabricRestApi.get_my_git_credentials(workspace['id'])

    git_creds = {
        'source': 'Automatic'
    }

    FabricRestApi.update_my_git_credentials(workspace['id'], git_creds)

    return FabricRestApi.get_git_status(workspace['id'])



def conn_filter(connection): 
    return connection['connectionDetails']['type'] ==  'GitHubSourceControl'

def delete_all_github_connections():
    """Delete All GitHub connection"""
    github_connections = list(filter(conn_filter, FabricRestApi.list_connections()))
    for connection in github_connections:
        AppLogger.log_step(f"Deleting connection {connection['displayName']}")
        FabricRestApi.delete_connection(connection['id'])

def create_github_connection(repo_url, target_workspace = None):
    """Create GitHub Connection"""
    target_workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)
    return FabricRestApi.create_github_connection(repo_url, target_workspace)

def connect_workspace_to_github_repo(target_workspace):
    workspace = FabricRestApi.get_workspace_by_name(target_workspace)
    github_repo_url = f'https://github.com/fabricdevcampdemos/{target_workspace}'
    github_connection = create_github_connection(github_repo_url)
    FabricRestApi.connect_workspace_to_github_repository(workspace, target_workspace)

def disconnect_workspace_from_github_repo(target_workspace):
    workspace = FabricRestApi.get_workspace_by_name(target_workspace)
    FabricRestApi.disconnect_workspace_from_git(workspace['id'])

WORKSPACE_NAME = 'Eva'
setup_workspace_with_github_repo(WORKSPACE_NAME)

# AppLogger.log_job_ended()
