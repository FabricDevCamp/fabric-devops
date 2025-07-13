"""deployment plan with parameters"""

from enum import Enum
from .environment_settings import EnvironmentSettings
from .app_logger import AppLogger

class DeploymentJobType(Enum):
    """Deployment Plan Type"""
    STAGED_DEPLOYMENT = 1
    CUSTOMER_TENANT = 2

class DeploymentJob:
    """Deployment Plan Class"""

    web_datasource_path_parameter = "web_datasource_path"
    adls_server_parameter = "adls_server"
    adls_container_name_parameter = "adls_container_name"
    adls_container_path_parameter = "adls_container_path"
    adls_account_key_parameter = "adls_account_key"

    def __init__(self, deployment_id, deployment_name,
                 deployment_type = DeploymentJobType.CUSTOMER_TENANT):
        self.deployment_type = deployment_type
        self.id = deployment_id
        self.name = deployment_name
        self.description = None
        self._parameters = dict()

        # setup Web datasource path
        self.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                      EnvironmentSettings.WEB_DATASOURCE_ROOT_URL + 'dev/')

        # setup ADLS datasource path
        self.set_deployment_parameter(DeploymentJob.adls_server_parameter,
                                      EnvironmentSettings.AZURE_STORAGE_SERVER)
        self.set_deployment_parameter(DeploymentJob.adls_container_name_parameter,
                                      EnvironmentSettings.AZURE_STORAGE_CONTAINER)
        self.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                      EnvironmentSettings.AZURE_STORAGE_CONTAINER)

    @property
    def target_workspace_name(self):
        """Target Workspace Name"""
        return f"Tenant - {self.id}"

    @property
    def parameters(self):
        """parameters collection"""
        return self._parameters

    def set_deployment_parameter(self, parameter_name, deployment_value):
        """add deployment parameter"""
        self._parameters.update({ parameter_name: deployment_value })

    def display_deployment_parameters(self, parameter_filter = None):
        """Display Deployment Parameters"""
        AppLogger.log_table_header(f'Deployment parameters for [{self.id}]')
        for key, value in self.parameters.items():
            if (parameter_filter is None) or \
               (parameter_filter in key):
                AppLogger.log_table_row(key, value)
 