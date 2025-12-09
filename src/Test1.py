"""Deploy Demo Solution with ADO GIT Intergation"""
import json
from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi, ItemDefinitionFactory

# project_name = "Project1"
# AdoProjectManager.create_project(project_name)
# AdoProjectManager.create_branch(project_name, 'test', 'main')
# AdoProjectManager.create_branch(project_name, 'dev', 'test')
# AdoProjectManager.set_default_branch(project_name, 'dev')


repo_name = "Project2"
GitHubRestApi.create_repository(repo_name)
GitHubRestApi.create_branch(repo_name, 'test', 'main')
GitHubRestApi.create_branch(repo_name, 'dev', 'test')
GitHubRestApi.set_default_branch(repo_name, 'dev')
