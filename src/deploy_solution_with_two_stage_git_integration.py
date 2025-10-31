"""Deploy Demo Solution with ADO GIT Intergation"""
import os

from fabric_devops import DeploymentManager, EnvironmentSettings, StagingEnvironments,\
                          AdoProjectManager, FabricRestApi, GitHubRestApi, AppLogger

if os.getenv("RUN_AS_SERVICE_PRINCIPAL") == 'true':
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = True
else:
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

SOLUTION_NAME = os.getenv("SOLUTION_NAME")
PROJECT_NAME = os.getenv("PROJECT_NAME")

DEV_WORKSPACE_NAME = F'{PROJECT_NAME}-dev'
PROD_WORKSPACE_NAME = F'{PROJECT_NAME}'

dev_deploy_job = StagingEnvironments.get_dev_environment()
prod_deploy_job = StagingEnvironments.get_prod_environment()

dev_workspace = DeploymentManager.deploy_solution_by_name(
    SOLUTION_NAME, 
    DEV_WORKSPACE_NAME,
    dev_deploy_job)

prod_workspace = FabricRestApi.create_workspace(PROD_WORKSPACE_NAME)


match os.getenv("GIT_INTEGRATION_PROVIDER"):

    case 'Azure DevOps':
      # create ADO project and connect project main repo to workspace
        DeploymentManager.setup_two_stage_ado_repo(
            dev_workspace,
            prod_workspace,
            PROJECT_NAME)

        # create feature1 workspace
        FEATURE1_NAME = 'feature1'
        FEATURE1_WORKSPACE_NAME = F'{DEV_WORKSPACE_NAME}-{FEATURE1_NAME}'
        FEATURE1_WORKSPACE = FabricRestApi.create_workspace(FEATURE1_WORKSPACE_NAME)

        # create feature1 branch and connect to feature1 workspace
        AdoProjectManager.create_branch(PROJECT_NAME, FEATURE1_NAME, 'dev')
        FabricRestApi.connect_workspace_to_ado_repo(FEATURE1_WORKSPACE, PROJECT_NAME, FEATURE1_NAME)

        # apply post sync/deploy fixes to feature1 workspace
        DeploymentManager.apply_post_deploy_fixes(
            FEATURE1_WORKSPACE_NAME,
            StagingEnvironments.get_dev_environment(),
            True)

        FabricRestApi.commit_workspace_to_git(
            FEATURE1_WORKSPACE['id'],
            commit_comment = 'Sync updates from feature workspace to repo after applying fixes')

        AdoProjectManager.create_and_merge_pull_request(PROJECT_NAME, FEATURE1_NAME , 'dev')
        AdoProjectManager.create_and_merge_pull_request(PROJECT_NAME, 'dev', 'main')

        AppLogger.log_job_complete(FEATURE1_WORKSPACE['id'])
        
    case 'GitHub':
        # create GitHub repo and connect to workspace
        repo_name = PROJECT_NAME.replace(" ", "-")
        
        DeploymentManager.setup_two_stage_github_repo(
            dev_workspace,
            prod_workspace,
            repo_name)

        # create feature1 workspace
        FEATURE1_NAME = 'feature1'
        FEATURE1_WORKSPACE_NAME = F'{DEV_WORKSPACE_NAME}-{FEATURE1_NAME}'
        FEATURE1_WORKSPACE = FabricRestApi.create_workspace(FEATURE1_WORKSPACE_NAME)

        # create feature1 branch and connect to feature1 workspace
        GitHubRestApi.create_branch(repo_name,  FEATURE1_NAME)
        FabricRestApi.connect_workspace_to_github_repo(FEATURE1_WORKSPACE, repo_name, FEATURE1_NAME)

        # apply post sync/deploy fixes to feature1 workspace
        DeploymentManager.apply_post_deploy_fixes(
            FEATURE1_WORKSPACE_NAME,
            StagingEnvironments.get_dev_environment(),
            True)

        FabricRestApi.commit_workspace_to_git(
            FEATURE1_WORKSPACE['id'],
            commit_comment = 'Sync updates from feature workspace to repo after applying fixes')

        GitHubRestApi.create_and_merge_pull_request(
            repo_name, 
            FEATURE1_NAME,
            'dev',
            'Push feature 1 changes', 
            'Push feature 1 changes')
        
        GitHubRestApi.create_and_merge_pull_request(
            repo_name, 
            'dev', 
            'main', 
            'Push feature 1 changes to prod', 
            'Push feature 1 changes to prod')

        AppLogger.log_job_complete(FEATURE1_WORKSPACE['id'])