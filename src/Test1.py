"""Deploy Demo Solution with ADO GIT Intergation"""

from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi

repo_name = 'Product-Sales'

GitHubRestApi.create_repository_variable(repo_name, 'web_data_url', 'bar')