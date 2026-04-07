"""Deploy solution to new workspace"""
import time


from fabric_devops import  GitHubRestApi


PROJECT_NAME = 'Acme'

repo_name = PROJECT_NAME


repo = GitHubRestApi.create_repository(PROJECT_NAME)

time.sleep(8)


GitHubRestApi.create_environment(repo_name, 'dev')

GitHubRestApi.create_environment(repo_name, 'test', add_reviewers=True)

GitHubRestApi.create_environment(repo_name, 'prod', add_reviewers=True)


# GitHubRestApi.create_workspace_readme(repo_name, 'main')

        
# GitHubRestApi.create_variables_for_github_project(
#     repo_name,
#     '11111111-1111-1111-1111-111111111111',
#     '22222222-2222-2222-2222-222222222222',
#     '33333333-3333-3333-3333-333333333333'
# )

# GitHubRestApi.copy_files_from_folder_to_repo(
#     repo_name,
#     'main',
#     'github_setup_with_fabric_cicd_and_release_flow'
# )
        

