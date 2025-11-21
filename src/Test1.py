"""Deploy Demo Solution with ADO GIT Intergation"""
import json
from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi, ItemDefinitionFactory

REPO_NAME = 'Brando'

repo = GitHubRestApi().create_repository(REPO_NAME)

test_branch =GitHubRestApi.create_branch(REPO_NAME, 'test', "main")
dev_branch = GitHubRestApi.create_branch(REPO_NAME, 'dev', "test")
GitHubRestApi.set_default_branch(REPO_NAME, 'dev')

print( json.dumps( dev_branch, indent=4 )   )

#print( json.dumps( branch, indent=4 )  )
