"""sample staging environments"""

from .app_settings import AppSettings
from .deployment_job import DeploymentJob, DeploymentJobType

class StagingEnvironments:
    """Sample staging environments"""

    @classmethod
    def get_dev_environment(cls) -> DeploymentJob:
        """Getter for dev environment"""
        deployment = DeploymentJob("dev", "dev",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "dev environment"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Dev/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Dev/")

        return deployment

    @classmethod
    def get_test_environment(cls) -> DeploymentJob:
        """Getter for test environment"""
        deployment = DeploymentJob("test", "test",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "test environment"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Test/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Test/")

        return deployment

    @classmethod
    def get_prod_environment(cls) -> DeploymentJob:
        """Getter for prod environment"""
        deployment = DeploymentJob("prod", "prod",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "prod environment"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Prod/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Prod/")

        return deployment
