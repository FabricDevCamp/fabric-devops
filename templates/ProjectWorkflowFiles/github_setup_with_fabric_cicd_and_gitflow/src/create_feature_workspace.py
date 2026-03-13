"""Create feature workspace from new ADO branch"""
import os
from fabric_devops_utils import EnvironmentSettings, AppLogger, FabricRestApi, DeploymentManager

REPO_NAME = EnvironmentSettings.GITHUB_REPOSITORY_NAME
FEATURE_NAME = os.getenv("FEATURE_NAME")

feature_workspace = DeploymentManager.create_feature_workspace_in_github(
    REPO_NAME,
    FEATURE_NAME,
    'dev'
)

AppLogger.log_substep('Adding workspace role of [Member] for developers group')
FabricRestApi.add_workspace_role_assignment_for_group(
    feature_workspace.id,
    EnvironmentSettings.DEVELOPERS_GROUP_ID,
    'Member'
)

AppLogger.log_job_complete(feature_workspace.id)
