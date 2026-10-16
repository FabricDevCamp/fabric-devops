"""Setup project with fabric-cicd and release flow """
import json
import os

from fabric_devops import DeploymentManager,  AppLogger, StagingEnvironments,\
                          FabricRestApi, AdoProjectManager, GitHubRestApi 

RUN_CLEANUP_ENVIRONMENT = os.getenv("RUN_CLEANUP_ENVIRONMENT") == 'true'
SETUP_ADO_DEMOS = os.getenv("SETUP_ADO_DEMOS") == 'true'
SETUP_GITHUB_DEMOS = os.getenv("SETUP_GITHUB_DEMOS") == 'true'

if RUN_CLEANUP_ENVIRONMENT:
    AppLogger.log_job("Running cleanup environment")
    DeploymentManager.cleanup_dev_environment()

if SETUP_ADO_DEMOS:
    AppLogger.log_job("Setting up ADO demos")
    solutions = ['Power BI Solution', 'Medallion Solution']
    for solution in solutions:
        workspace_name_root = solution.replace("Solution", "").replace(" ", "")
        DeploymentManager.setup_ado_repo_with_fabric_cicd_and_gitflow(solution, f'{workspace_name_root}_GitFlow')
        DeploymentManager.setup_ado_repo_with_fabric_cicd_and_github_flow(solution, f'{workspace_name_root}_TBD')                                                                             
        DeploymentManager.setup_ado_repo_with_fabric_cicd_and_release_flow(solution, f'{workspace_name_root}_ReleaseFlow')

        api_deployed_workspace = f'{workspace_name_root}'
        DeploymentManager.deploy_solution_by_name(
            solution,
            f'{api_deployed_workspace}_ado', 
            deploy_using_fabric_cicd=False)

        DeploymentManager.setup_ado_repo_with_fabric_cicd_and_gitflow(
            api_deployed_workspace,
            api_deployed_workspace
        )

if SETUP_GITHUB_DEMOS:
    AppLogger.log_job("Setting up GitHub demos")
    solutions = ['Power BI Solution', 'Medallion Solution']
    for solution in solutions:
        workspace_name_root = solution.replace("Solution", "").replace(" ", "")
        DeploymentManager.setup_github_repo_with_fabric_cicd_and_gitflow(solution, f'{workspace_name_root}_GitFlow')
        DeploymentManager.setup_github_repo_with_fabric_cicd_and_github_flow(solution, f'{workspace_name_root}_TBD')                                                                             
        DeploymentManager.setup_github_repo_with_fabric_cicd_and_github_flow(solution, f'{workspace_name_root}_ReleaseFlow')
            
        api_deployed_workspace = f'{workspace_name_root}_Api'
        DeploymentManager.deploy_solution_by_name(
            solution,
            f'{api_deployed_workspace}_github', 
            deploy_using_fabric_cicd=False)

        DeploymentManager.setup_ado_repo_with_fabric_cicd_and_gitflow(
            api_deployed_workspace,
            api_deployed_workspace
        )
