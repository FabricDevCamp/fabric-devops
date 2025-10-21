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
        # create ADO project and connect project main repo to workspace
        AdoProjectManager.create_project(workspace_name)
        FabricRestApi.connect_workspace_to_ado_repo(workspace, workspace_name)

        AdoProjectManager.copy_files_from_folder_to_repo(
            workspace_name,
            'main', 
            'ADO_SetUpForGitIntegration',
        )
        

        # create feature1 workspace
        FEATURE1_NAME = 'feature1'
        FEATURE1_WORKSPACE_NAME = F'{workspace_name} - {FEATURE1_NAME}'
        FEATURE1_WORKSPACE = FabricRestApi.create_workspace(FEATURE1_WORKSPACE_NAME)

        # create feature1 branch and connect to feature1 workspace
        AdoProjectManager.create_branch(workspace_name, FEATURE1_NAME, 'main')
        FabricRestApi.connect_workspace_to_ado_repo(FEATURE1_WORKSPACE, workspace_name, FEATURE1_NAME)

       # apply post sync/deploy fixes to feature1 workspace
        DeploymentManager.apply_post_sync_fixes(
            FEATURE1_WORKSPACE_NAME,
            StagingEnvironments.get_dev_environment(),
            True
        )

        # create feature2 workspace
        FEATURE2_NAME = 'feature2'
        FEATURE2_WORKSPACE_NAME = F'{workspace_name} - {FEATURE2_NAME}'
        FEATURE2_WORKSPACE = FabricRestApi.create_workspace(FEATURE2_WORKSPACE_NAME)

        # create feature2 branch and connect to feature2 workspace
        AdoProjectManager.create_branch(workspace_name, FEATURE2_NAME, 'main')
        FabricRestApi.connect_workspace_to_ado_repo(FEATURE2_WORKSPACE, workspace_name, FEATURE2_NAME)

        # not applying post sync/deploy fixes to feature2 workspace to see what happens

        
        
    case 'GitHub':
        # create GitHub repo and connect to workspace
        repo_name = workspace_name.replace(" ", "-")
        GitHubRestApi.create_repository(repo_name)
        FabricRestApi.connect_workspace_to_github_repo(workspace, repo_name)

        # create workspace for feature1
        FEATURE1_NAME = 'feature1'
        FEATURE1_WORKSPACE_NAME = F'{workspace_name} - {FEATURE1_NAME}'
        FEATURE1_WORKSPACE = FabricRestApi.create_workspace(FEATURE1_WORKSPACE_NAME)

        # create feature branch and connect to feature workspace
        GitHubRestApi.create_branch(repo_name, FEATURE1_NAME)
        FabricRestApi.connect_workspace_to_github_repo(FEATURE1_WORKSPACE, repo_name, FEATURE1_NAME)

        # create workspace for feature2
        FEATURE2_NAME = 'feature2'
        FEATURE2_WORKSPACE_NAME = F'{workspace_name} - {FEATURE2_NAME}'
        FEATURE2_WORKSPACE = FabricRestApi.create_workspace(FEATURE2_WORKSPACE_NAME)

        # create feature branch and connect to feature workspace
        GitHubRestApi.create_branch(repo_name, FEATURE2_NAME)
        FabricRestApi.connect_workspace_to_github_repo(FEATURE2_WORKSPACE, repo_name, FEATURE2_NAME)

        # apply post sync/deploy fixes to feature2 workspace, but not feature1 workspace
        DeploymentManager.apply_post_sync_fixes(
            FEATURE2_WORKSPACE_NAME,
            StagingEnvironments.get_dev_environment(),
            True)

AppLogger.log_job_complete(workspace['id'])
