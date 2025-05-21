"""Setup GIT Connection"""

import os

from fabric_devops import DeploymentManager, AppSettings, FabricRestApi

WORKSPACE = None

match os.getenv("SOLUTION_NAME"):

    case 'Custom Power BI Solution':
        WORKSPACE = DeploymentManager.deploy_powerbi_solution(
            'Custom Power BI Solution')

    case 'Custom Notebook Solution':
        WORKSPACE = DeploymentManager.deploy_notebook_solution(
            'Custom Notebook Solution')

    case 'Custom Shortcut Solution':
        WORKSPACE = DeploymentManager.deploy_shortcut_solution(
            'Custom Shortcut Solution')

    case 'Custom Data Pipeline Solution':
        WORKSPACE = DeploymentManager.deploy_data_pipeline_solution(
            'Custom Data Pipeline Solution')

    case 'Custom Warehouse Solution':
        WORKSPACE = DeploymentManager.deploy_warehouse_solution(
            'Custom Warehouse Solution')
        
    case 'Custom Realtime Solution':
        WORKSPACE = DeploymentManager.deploy_realtime_solution(
            'Custom Realtime Solution')

    case 'Custom Variable Library Solution':
        # SPN bug with var lib requires user auth
        AppSettings.RUN_AS_SERVICE_PRINCIPAL = False
        FabricRestApi.authenticate()
        WORKSPACE = DeploymentManager.deploy_variable_library_solution(
            'Custom Variable Library Solution')

DeploymentManager.connect_workspace_to_github_repo(WORKSPACE)
