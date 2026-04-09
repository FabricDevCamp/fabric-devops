"""Create test release build"""
from datetime import datetime
from zoneinfo import ZoneInfo
from fabric_devops_utils import EnvironmentSettings, AppLogger, FabricRestApi, GitHubRestApi

AppLogger.log_job("Creating and deploying test build")

REPO_NAME = EnvironmentSettings.GITHUB_REPOSITORY_NAME

time_now = datetime.now(ZoneInfo("America/New_York"))
time_now_formatted = time_now.strftime("%Y-%m-%d-%H-%M")
TEST_BRANCH_NAME = str(f'test-{time_now_formatted}')

print(f'Creating branch: {TEST_BRANCH_NAME}')

GitHubRestApi.create_branch(REPO_NAME, TEST_BRANCH_NAME, 'main')

FabricRestApi.update_workspace_description(
    EnvironmentSettings.WORKSPACE_ID_TEST,
    f"BUILD: {TEST_BRANCH_NAME}")
