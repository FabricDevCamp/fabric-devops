"""Deploy Demo Solution with ADO GIT Intergation"""
import json
from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi

repo_name = 'Apollo'

GitHubRestApi.create_and_merge_pull_request(
    repo_name, 'feature1', 'dev', "yup", 'yup yup'
)


