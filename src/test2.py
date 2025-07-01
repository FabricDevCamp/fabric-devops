
from fabric_devops import GitHubRestApi

PROJECT_NAME = 'Apollo'

DEPLOYMENT_PIPELINE_NAME = PROJECT_NAME
DEV_WORKSPACE_NAME = f"{PROJECT_NAME}-dev"
TEST_WORKSPACE_NAME = f"{PROJECT_NAME}-test"
PROD_WORKSPACE_NAME = f"{PROJECT_NAME}"

pull_request = GitHubRestApi.create_pull_request(PROJECT_NAME, 'dev', 'test')

pull_request_id = pull_request['number']

GitHubRestApi.merge_pull_request(
    PROJECT_NAME, 
    pull_request_id, 
    "Initial merge",
    "here it is")