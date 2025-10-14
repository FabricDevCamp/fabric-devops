"""Deploy Solution"""
import os

from fabric_devops import DeploymentManager, EnvironmentSettings, AppLogger, StagingEnvironments,\
                          AdoProjectManager, FabricRestApi, GitHubRestApi

if os.getenv("RUN_AS_SERVICE_PRINCIPAL") == 'true':
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = True
else:
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

solution_name = os.getenv("SOLUTION_NAME")
workspace_name = solution_name

match os.getenv("TARGET_ENVIRONMENT"):
    case 'Dev':
        deploy_job = StagingEnvironments.get_dev_environment()
    case 'Test':
        deploy_job = StagingEnvironments.get_test_environment()
    case 'Prod':
        deploy_job = StagingEnvironments.get_prod_environment()

workspace = DeploymentManager.deploy_solution_by_name(solution_name, workspace_name, deploy_job)

match os.getenv("GIT_INTEGRATION_PROVIDER"):
    case 'Azure DevOps':
        AdoProjectManager.create_project(workspace_name)
        FabricRestApi.connect_workspace_to_ado_repo(workspace, workspace_name)
    case 'GitHub':
        repo_name = workspace_name.replace(" ", "-")
        GitHubRestApi.create_repository(repo_name)
        FabricRestApi.connect_workspace_to_github_repo(workspace, repo_name)

        # create and connect feature workspace and feature branch
        FEATURE_NAME = 'feature1'
        FEATURE_WORKSPACE_NAME = F'{workspace_name} - {FEATURE_NAME}'
        FEATURE_WORKSPACE = FabricRestApi.create_workspace(FEATURE_WORKSPACE_NAME)
        GitHubRestApi.create_branch(repo_name, FEATURE_NAME)
        FabricRestApi.connect_workspace_to_github_repo(FEATURE_WORKSPACE, repo_name, FEATURE_NAME)

        DeploymentManager.apply_post_deploy_fixes(
            FEATURE_WORKSPACE_NAME,
            StagingEnvironments.get_dev_environment(),
            True)




AppLogger.log_job_complete(workspace['id'])
