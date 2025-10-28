import json
import os
from fabric_devops import DeploymentManager, EnvironmentSettings, FabricRestApi, AdoProjectManager, GitHubRestApi


PROJECT_NAME = 'Brenda'

# create first feature workspace
FEATURE_NAME = 'feature2'
FEATURE_WORKSPACE_NAME = F'{PROJECT_NAME}-dev-{FEATURE_NAME}'
FEATURE_WORKSPACE = FabricRestApi.create_workspace(FEATURE_WORKSPACE_NAME)

AdoProjectManager.create_branch(PROJECT_NAME, FEATURE_NAME, "dev")
FabricRestApi.connect_workspace_to_ado_repo(FEATURE_WORKSPACE, PROJECT_NAME, FEATURE_NAME)

DeploymentManager.apply_post_deploy_fixes(
    FEATURE_WORKSPACE_NAME,
    StagingEnvironments.get_dev_environment(),
    True)

FabricRestApi.commit_workspace_to_git(FEATURE_WORKSPACE)

