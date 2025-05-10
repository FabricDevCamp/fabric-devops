"""Setup GIT Connection"""

import os

from fabric_devops import DeploymentManager

WORKSPACE = None

match os.getenv("SOLUTION_NAME"):

    case 'Custom Power BI Solution':
        WORKSPACE = DeploymentManager.deploy_powerbi_solution('Custom Power BI Solution')

    case 'Custom Notebook Solution':
        WORKSPACE = DeploymentManager.deploy_notebook_solution('Custom Notebook Solution')

    case 'Custom Shortcut Solution':
        WORKSPACE = DeploymentManager.deploy_shortcut_solution('Custom Shortcut Solution')

    case 'Custom Data Pipeline Solution':
        WORKSPACE = DeploymentManager.deploy_data_pipeline_solution('Custom Data Pipeline Solution')

    case 'Custom Variable Library Solution':
        WORKSPACE = DeploymentManager.deploy_variable_library_solution('Custom Variable Library Solution')

DeploymentManager.connect_workspace_to_github_repo(WORKSPACE)

