"""sample customer data"""

from typing import List

from .environment_settings import EnvironmentSettings
from .deployment_job import DeploymentJob, DeploymentJobType

class SampleCustomerData:
    """Sample Customer Data"""

    @classmethod
    def get_adventureworks(cls) -> DeploymentJob:
        """Getter for adventure works"""
        deployment = DeploymentJob("AdventureWorks", "Adventure Works",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "The ultimate provider for the avid bike rider"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            EnvironmentSettings.WEB_DATASOURCE_ROOT_URL +
                                            'customers/adventureworks/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/productsales/customers/adventureworks/")

        return deployment

    @classmethod
    def get_contoso(cls) -> DeploymentJob:
        """Getter for contoso"""
        deployment = DeploymentJob("Contoso", "Contoso Inc",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "The ultimate provider for the avid bike rider"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            EnvironmentSettings.WEB_DATASOURCE_ROOT_URL +
                                            'customers/contoso/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/productsales/customers/contoso/")

        return deployment

    @classmethod
    def get_fabrikam(cls) -> DeploymentJob:
        """Getter for fabrikam"""
        deployment = DeploymentJob("Fabrikam", "Fabrikam Inc",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "The Absolute WHY and WHERE for Enterprise Hardware"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            EnvironmentSettings.WEB_DATASOURCE_ROOT_URL +
                                            'customers/fabrikam/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/productsales/customers/fabrikam/")

        return deployment

    @classmethod
    def get_northwind(cls) -> DeploymentJob:
        """Getter for Northwind"""
        deployment = DeploymentJob("Northwind", "Northwind Traders",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "Microsoft's favorate fictional company"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            EnvironmentSettings.WEB_DATASOURCE_ROOT_URL +
                                            'customers/northwind/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/productsales/customers/northwind/")

        return deployment

    @classmethod
    def get_seamarkfarms(cls) -> DeploymentJob:
        """Getter for SeamarkFarms"""
        deployment = DeploymentJob("SeamarkFarms", "Seamark Farms",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "Sweet Sheep for Cheap"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            EnvironmentSettings.WEB_DATASOURCE_ROOT_URL +
                                            'customers/seamarkfarms/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/productsales/customers/seamarkfarms/")

        return deployment

    @classmethod
    def get_wingtip(cls) -> DeploymentJob:
        """Getter for Wingtip"""
        deployment = DeploymentJob("Wingtip", "Wingtip Toys",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "Retro toys for nostalgic girls and boys"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            EnvironmentSettings.WEB_DATASOURCE_ROOT_URL +
                                            'customers/wingtip/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/productsales/customers/wingtip/")

        return deployment

    @classmethod
    def get_all_customers(cls) -> List[DeploymentJob]:
        """Get All Customers"""
        return [
            cls.get_adventureworks(),
            cls.get_contoso(),
            cls.get_fabrikam(),
            cls.get_northwind(),
            cls.get_seamarkfarms(),
            cls.get_wingtip()
        ]
