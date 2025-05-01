"""sample customer data"""

from .app_settings import AppSettings
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
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Customers/AdventureWorks/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Customers/AdventureWorks/")

        return deployment

    @classmethod
    def get_contoso(cls) -> DeploymentJob:
        """Getter for contoso"""
        deployment = DeploymentJob("Contoso", "Contoso Inc",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "The ultimate provider for the avid bike rider"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Customers/Contoso/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Customers/Contoso/")

        return deployment

    @classmethod
    def get_fabrikam(cls) -> DeploymentJob:
        """Getter for fabrikam"""
        deployment = DeploymentJob("Fabrikam", "Fabrikam Inc",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "The Absolute WHY and WHERE for Enterprise Hardware"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Customers/Fabrikam/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Customers/Fabrikam/")

        return deployment

    @classmethod
    def get_northwind(cls) -> DeploymentJob:
        """Getter for Northwind"""
        deployment = DeploymentJob("Northwind", "Northwind Traders",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "Microsoft's favorate fictional company"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Customers/Northwind/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Customers/Northwind/")

        return deployment

    @classmethod
    def get_seamarkfarms(cls) -> DeploymentJob:
        """Getter for SeamarkFarms"""
        deployment = DeploymentJob("SeamarkFarms", "Seamark Farms",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "Sweet Sheep for Cheap"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Customers/SeamarkFarms/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Customers/SeamarkFarms/")

        return deployment

    @classmethod
    def get_wingtip(cls) -> DeploymentJob:
        """Getter for Wingtip"""
        deployment = DeploymentJob("Wingtip", "Wingtip Toys",
                                    DeploymentJobType.CUSTOMER_TENANT)
        deployment.description = "Retro toys for nostalgic girls and boys"

        # setup Web datasource path
        deployment.set_deployment_parameter(DeploymentJob.web_datasource_path_parameter,
                                            AppSettings.WEB_DATASOURCE_ROOT_URL +
                                            'Customers/Wingtip/')

        # setup ADLS datasource path
        deployment.set_deployment_parameter(DeploymentJob.adls_container_path_parameter,
                                            "/ProductSales/Customers/Wingtip/")

        return deployment
