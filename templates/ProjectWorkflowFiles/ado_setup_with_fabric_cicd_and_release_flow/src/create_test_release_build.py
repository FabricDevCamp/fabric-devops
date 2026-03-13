"""Create test release build"""
from datetime import datetime
from zoneinfo import ZoneInfo
from fabric_devops_utils import EnvironmentSettings, AppLogger, FabricRestApi, AdoProjectManager

AppLogger.log_job("Creating and deploying test build")

PROJECT_NAME = EnvironmentSettings.ADO_PROJECT_NAME

time_now = datetime.now(ZoneInfo("America/New_York"))
time_now_formatted = time_now.strftime("%Y-%m-%d-%H-%M")
TEST_BRANCH_NAME = str(f'test-{time_now_formatted}')

print(f'Creating branch: {TEST_BRANCH_NAME}')

AdoProjectManager.create_branch(PROJECT_NAME, TEST_BRANCH_NAME, 'main')

FabricRestApi.update_workspace_description(
    EnvironmentSettings.WORKSPACE_ID_TEST,
    f"BUILD: {TEST_BRANCH_NAME}")
