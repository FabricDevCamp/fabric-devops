"""Deploy Demo Solution with ADO GIT Intergation"""

from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi

repo_name = 'Bixby'

GitHubRestApi.create_feature_branch(repo_name, 'feature2')