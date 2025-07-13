"""Test3"""

from fabric_devops import StagingEnvironments, DeploymentManager

DeploymentManager.deploy_notebook_solution_with_variable(
    'Contoso1',
    StagingEnvironments.get_dev_environment()
)