"""Deployment Manager"""

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import yaml

from .app_logger import AppLogger
from .environment_settings import EnvironmentSettings
from .fabric_rest_api import FabricRestApi, PowerBiRestApi
from .fabric_cicd_manager import FabricCicdManager
from .item_definition_factory import ItemDefinitionFactory
from .staging_environments import StagingEnvironments
from .deployment_job import DeploymentJob, DeploymentJobType
from .variable_library import VariableLibrary, Valueset
from .ado_project_manager import AdoProjectManager
from .github_rest_api import GitHubRestApi

class DeploymentManager:
    """Deployment Manager"""

    #region Deploy solution by name

    @classmethod
    def deploy_solution_by_name(cls,
                                solution_name, 
                                target_workspace = None,
                                deploy_job = StagingEnvironments.get_dev_environment(),
                                deploy_using_fabric_cicd = True):
        """Deploy Solution by Name"""

        if target_workspace is None:
            target_workspace = solution_name
            
        if deploy_using_fabric_cicd:                
            match solution_name:
                case 'Power BI Solution':
                    return cls.deploy_powerbi_solution(target_workspace, deploy_job)
                case 'Notebook Solution':
                    return cls.deploy_notebook_solution(target_workspace, deploy_job)
                case "Shortcut Solution":
                    return cls.deploy_shortcut_solution(target_workspace, deploy_job)
                case "Pipeline Solution":
                    return cls.deploy_pipeline_solution(target_workspace, deploy_job)
                case "Medallion Solution":
                    return cls.deploy_medallion_solution(target_workspace, deploy_job)                
                case _:
                    raise LookupError(f'Unknown solution name [{solution_name}]')
    
        else:
            match solution_name:
                case 'Power BI Solution':
                    return cls.deploy_powerbi_solution_using_apis(target_workspace, deploy_job)
                case 'Notebook Solution':
                    return cls.deploy_notebook_solution_using_apis(target_workspace, deploy_job)
                case "Shortcut Solution":
                    return cls.deploy_shortcut_solution_using_apis(target_workspace, deploy_job)
                case "Pipeline Solution":
                    return cls.deploy_pipeline_solution_using_apis(target_workspace, deploy_job)
                case "Medallion Solution":
                    return cls.deploy_medallion_solution_using_apis(target_workspace, deploy_job)
                case _:
                    raise LookupError(f'Unknown solution name [{solution_name}]')
            
    @classmethod
    def update_solution_post_deploy(cls, target_workspace, solution_name):
        """Update solution post deploy"""
        match solution_name:
            case 'Power BI Solution':
                return cls.update_powerbi_solution_post_deploy(target_workspace)
            case 'Notebook Solution':
                return cls.update_notebook_solution_post_deploy(target_workspace)
            case "Shortcut Solution":
                return cls.update_shortcut_solution_post_deploy(target_workspace)
            case "Pipeline Solution":
                return cls.update_pipeline_solution_post_deploy(target_workspace)
            case "Medallion Solution":
                return cls.update_medallion_solution_post_deploy(target_workspace)
            
            # raise exception if there is no matching solution
            case _:
                raise LookupError(f'Unknown solution name [{solution_name}]')

    #endregion

    #region Variable library support

    @classmethod
    def create_web_datasource_url_variable_library(cls, workspace, deploy_job: DeploymentJob, staging_folder_id = None):
        """Create Variable Library for web datasource url"""
        
        if deploy_job.deployment_type == DeploymentJobType.CUSTOMER_TENANT:
            # use customer-specific values for tenant workspace parameterization
            AppLogger.log_step(f'Creating variable library for customer[{deploy_job.name}]')
            web_datasource_path = deploy_job.parameters[DeploymentJob.web_datasource_path_parameter]
            
            variable_library = VariableLibrary()
            variable_library.add_variable("web_datasource_path", web_datasource_path)
            
            create_library_request = ItemDefinitionFactory.get_variable_library_create_request(
                "environment_settings",
                variable_library)

            return FabricRestApi.create_item(
                    workspace.id,
                    create_library_request,
                    staging_folder_id)
            
        if deploy_job.deployment_type == DeploymentJobType.STAGED_DEPLOYMENT:
            # create variable library default values for dev and value sets for test and prod
            AppLogger.log_step('Creating variable library for staged deployments')

            # this determines which value set to activate after crearting variable library
            target_env = deploy_job.name

            # create variable library
            variable_library = VariableLibrary()
                        
            # use default values for dev environment
            dev_env = StagingEnvironments.get_dev_environment()
            dev_datasource_path = dev_env.parameters[DeploymentJob.web_datasource_path_parameter]
            variable_library.add_variable("web_datasource_path", dev_datasource_path)
        
            # add value set for test environment
            test_env = StagingEnvironments.get_test_environment()        
            test_datasource_path = test_env.parameters[DeploymentJob.web_datasource_path_parameter]
            test_value_set = Valueset('test')
            test_value_set.add_variable_override('web_datasource_path', test_datasource_path)        
            variable_library.add_valueset(test_value_set)

            # add value set for prod environment
            prod_env = StagingEnvironments.get_prod_environment()
            prod_datasource_path = prod_env.parameters[DeploymentJob.web_datasource_path_parameter]
            prod_value_set = Valueset('prod')
            prod_value_set.add_variable_override('web_datasource_path', prod_datasource_path)        
            variable_library.add_valueset(prod_value_set)

            create_library_request = ItemDefinitionFactory.get_variable_library_create_request(
                "environment_settings",
                variable_library
            )

            variable_library = FabricRestApi.create_item(
                workspace.id,
                create_library_request,
                staging_folder_id)
            
            FabricRestApi.set_active_valueset_for_variable_library(
                workspace.id,
                variable_library,
                target_env
            )
            
            return variable_library

    @classmethod
    def create_adls_connection_and_variable_library(
        cls, 
        workspace, 
        staging_folder_id = None, 
        deploy_job: DeploymentJob = StagingEnvironments.get_dev_environment()):
        """Create Vairable Library with ADLS settings"""
        
        if deploy_job.deployment_type == DeploymentJobType.CUSTOMER_TENANT:
            # use customer-specific values for tenant workspace parameterization
            AppLogger.log_step(f'Creating ADLS Gen2 connection for customer[{deploy_job.name}]')
            adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
            adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
            adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
            adls_path = adls_container_name + adls_container_path
            
            connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                adls_server,
                adls_path)

            variable_library = VariableLibrary()
            variable_library.add_variable("adls_server", adls_server)
            variable_library.add_variable("adls_container_name", adls_container_name)
            variable_library.add_variable("adls_container_path", adls_container_path)
            variable_library.add_variable("adls_shortcut_subpath", adls_container_name + adls_container_path)
            variable_library.add_variable("adls_connection_id", connection.id)
            
            create_library_request = ItemDefinitionFactory.get_variable_library_create_request(
                "environment_settings",
                variable_library)

            return FabricRestApi.create_item(
                    workspace.id,
                    create_library_request,
                    staging_folder_id)
            
        if deploy_job.deployment_type == DeploymentJobType.STAGED_DEPLOYMENT:
            # create variable library default values for dev and value sets for test and prod
            AppLogger.log_step('Creating ADLS connections for staged deployment')

            # use default values for dev environment
            dev_deploy_job = StagingEnvironments.get_dev_environment()
            dev_adls_server = dev_deploy_job.parameters[DeploymentJob.adls_server_parameter]
            dev_adls_container_name = dev_deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
            dev_adls_container_path = dev_deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
            dev_adls_path = dev_adls_container_name + dev_adls_container_path
            
            dev_connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                dev_adls_server,
                dev_adls_path)
            
            dev_connection_id = dev_connection.id
            AppLogger.log_substep(f'Dev connection ID: {dev_connection_id}')

            variable_library = VariableLibrary()
            variable_library.add_variable("adls_server", dev_adls_server)
            variable_library.add_variable("adls_container_name", dev_adls_container_name)
            variable_library.add_variable("adls_container_path", dev_adls_container_path)
            variable_library.add_variable("adls_shortcut_subpath", dev_adls_container_name + dev_adls_container_path)
            variable_library.add_variable("adls_connection_id", dev_connection_id)
        
            # add value set for test environment
            test_deploy_job = StagingEnvironments.get_test_environment()        
            test_adls_server = test_deploy_job.parameters[DeploymentJob.adls_server_parameter]
            test_adls_container_name = test_deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
            test_adls_container_path = test_deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
            test_adls_path = test_adls_container_name + test_adls_container_path
            
            test_connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                test_adls_server,
                test_adls_path)
            
            test_connection_id = test_connection.id            
            AppLogger.log_substep(f'Test connection ID: {test_connection_id}')

            test_value_set = Valueset('test')
            test_value_set.add_variable_override('adls_server', test_adls_server)
            test_value_set.add_variable_override('adls_container_name', test_adls_container_name)
            test_value_set.add_variable_override('adls_container_path', test_adls_container_path)
            test_value_set.add_variable_override("adls_shortcut_subpath", test_adls_container_name + test_adls_container_path)
            test_value_set.add_variable_override('adls_connection_id', test_connection.id)
        
            variable_library.add_valueset(test_value_set)
    
            # prod
            prod_deploy_job = StagingEnvironments.get_prod_environment()
            prod_adls_server = prod_deploy_job.parameters[DeploymentJob.adls_server_parameter]
            prod_adls_container_name = prod_deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
            prod_adls_container_path = prod_deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
            prod_adls_path = prod_adls_container_name + prod_adls_container_path
            
            prod_connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                prod_adls_server,
                prod_adls_path)
            
            prod_connection_id = prod_connection.id
            AppLogger.log_substep(f'Prod connection ID: {prod_connection_id}')

            prod_value_set = Valueset('prod')
            prod_value_set.add_variable_override('adls_server', prod_adls_server)
            prod_value_set.add_variable_override('adls_container_name', prod_adls_container_name)
            prod_value_set.add_variable_override('adls_container_path', prod_adls_container_path)
            prod_value_set.add_variable_override("adls_shortcut_subpath", prod_adls_container_name + prod_adls_container_path)
            prod_value_set.add_variable_override('adls_connection_id', prod_connection.id)
        
            variable_library.add_valueset(prod_value_set)

            create_library_request = \
                ItemDefinitionFactory.get_variable_library_create_request(
                    "environment_settings",
                    variable_library
            )

            variable_library = FabricRestApi.create_item(
                workspace.id,
                create_library_request,
                staging_folder_id)
            
            FabricRestApi.set_active_valueset_for_variable_library(
                workspace.id,
                variable_library,
                deploy_job.name
            )
            
            return variable_library

    @classmethod
    def get_adls_connections(cls):
        """Get ADLS connections"""
        
        # create variable library default values for dev and value sets for test and prod
        AppLogger.log_step('Getting ADLS connections for staged deployment')

        # get or create ADLS connection for dev
        dev_deploy_job = StagingEnvironments.get_dev_environment()
        dev_adls_server = dev_deploy_job.parameters[DeploymentJob.adls_server_parameter]
        dev_adls_container_name = dev_deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        dev_adls_container_path = dev_deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        dev_adls_path = dev_adls_container_name + dev_adls_container_path
        
        dev_connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            dev_adls_server,
            dev_adls_path)

        # get or create ADLS connection for test
        test_deploy_job = StagingEnvironments.get_test_environment()        
        test_adls_server = test_deploy_job.parameters[DeploymentJob.adls_server_parameter]
        test_adls_container_name = test_deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        test_adls_container_path = test_deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        test_adls_path = test_adls_container_name + test_adls_container_path
        
        test_connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            test_adls_server,
            test_adls_path)

        # get or create ADLS connection for prod
        prod_deploy_job = StagingEnvironments.get_prod_environment()
        prod_adls_server = prod_deploy_job.parameters[DeploymentJob.adls_server_parameter]
        prod_adls_container_name = prod_deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        prod_adls_container_path = prod_deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        prod_adls_path = prod_adls_container_name + prod_adls_container_path
        
        prod_connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            prod_adls_server,
            prod_adls_path)
        
        return {
            'dev': dev_connection.id,
            'test': test_connection.id,
            'prod': prod_connection.id
        }

    #endregion

    #region Notebook support
    
    @classmethod
    def update_source_lakehouse_in_notebook(cls,
                workspace_name,
                notebook_name,
                lakehouse_name):
        """Update datasource path in notebook"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        notebook = FabricRestApi.get_item_by_name(workspace.id, notebook_name, 'Notebook')
        lakehouse = FabricRestApi.get_item_by_name(workspace.id, lakehouse_name, "Lakehouse")

        workspace_id = workspace.id
        lakehouse_id = lakehouse.id

        search_replace_terms = {
            r'("default_lakehouse"\s*:\s*)".*"': rf'\1"{lakehouse_id}"',
            r'("default_lakehouse_name"\s*:\s*)".*"': rf'\1"{lakehouse_name}"',
            r'("default_lakehouse_workspace_id"\s*:\s*)".*"': rf'\1"{workspace_id}"',
            r'("known_lakehouses"\s*:\s*)\[[\s\S]*?\]': rf'\1[{{"id": "{lakehouse_id}"}}]',
        }

        notebook_definition = FabricRestApi.get_item_definition(workspace.id, notebook).as_dict()

        notebook_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part_with_regex(
                notebook_definition['definition'],
                'notebook-content.py',
                search_replace_terms)
        }

        FabricRestApi.update_item_definition(workspace.id, notebook, notebook_definition)

    #endregion

    #region Semantic Model support
        
    @classmethod
    def update_imported_semantic_model_source(cls, workspace,
                                                semantic_model_name, 
                                                web_datasource_path):
        """Update Imported Sementic Model Source"""
        model = FabricRestApi.get_item_by_name(workspace.id, semantic_model_name, 'SemanticModel')
        old_web_datasource_path = PowerBiRestApi.get_web_url_from_semantic_model(workspace.id, model.id) + '/'

        if web_datasource_path == old_web_datasource_path:
            AppLogger.log_substep(f"Verified web datasource path: [{web_datasource_path}]")
        else:
            old_model_definition = FabricRestApi.get_item_definition(workspace.id, model).as_dict()
    
            search_replace_terms = {
                old_web_datasource_path: web_datasource_path
            }

            model_definition = {
                'definition': ItemDefinitionFactory.update_item_definition_part(
                    old_model_definition['definition'],
                    'definition/expressions.tmdl',
                    search_replace_terms)
            }

            FabricRestApi.update_item_definition(workspace.id, model, model_definition)

    @classmethod
    def update_directlake_semantic_model_source(cls,
                                                workspace_name,
                                                semantic_model_name,
                                                lakehouse_name):
        """Update DirectLake Sementic Model Source"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        lakehouse = FabricRestApi.get_item_by_name(workspace.id, lakehouse_name, 'Lakehouse')
        model = FabricRestApi.get_item_by_name(workspace.id, semantic_model_name, 'SemanticModel')
        
        old_sql_endpoint = FabricRestApi.get_sql_endpoint_from_semantic_model(
            workspace.id,
            model.id
        )

        new_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(
            workspace.id,
            lakehouse)

        old_model_definition = FabricRestApi.get_item_definition(workspace.id, model).as_dict()

        search_replace_terms = {
            old_sql_endpoint['server']: new_sql_endpoint['server']
        }

        model_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part(
                old_model_definition['definition'],
                'definition/expressions.tmdl',
                search_replace_terms)
        }

        FabricRestApi.update_item_definition(workspace.id, model, model_definition)

    #endregion

    #region Deploy solution using fabric-cicd library

    @classmethod
    def deploy_powerbi_solution(cls, target_workspace, 
                                deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Power BI Solution using fabric-cicd"""

        AppLogger.log_job(f"Deploying Power BI Solution to [{target_workspace}] using fabric-cicd")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Power BI Solution')
        
        fabric_solution_folder = 'Power BI Solution'
        FabricCicdManager.deploy(fabric_solution_folder, deploy_job.name, workspace.id)
     
        AppLogger.log_step("Applying post-deploy updates to Power BI solution")
  
        imported_model = FabricRestApi.get_item_by_name(workspace.id, 'Product Sales Imported Model', 'SemanticModel')
        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, imported_model.id)
                
        return workspace

    @classmethod
    def deploy_notebook_solution(cls, target_workspace,
                                 deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Notebook Solution using fabric-cicd"""
        
        AppLogger.log_job(f"Deploying Notebook Solution to [{target_workspace}] using fabric-cicd")

        workspace = FabricRestApi.create_workspace(target_workspace)
        
        FabricRestApi.update_workspace_description(workspace.id, 'Notebook Solution')
        
        cls.create_web_datasource_url_variable_library(workspace, deploy_job)
        
        fabric_solution_folder = 'Notebook Solution'
        FabricCicdManager.deploy(fabric_solution_folder, deploy_job.name, workspace.id)
        
        AppLogger.log_step("Applying post-deploy updates to notebook solution")
  
        notebook = FabricRestApi.get_item_by_name(workspace.id, 'Create Lakehouse Tables', 'Notebook')
        FabricRestApi.run_notebook(workspace.id, notebook)
        
        lakehouse = FabricRestApi.get_item_by_name(workspace.id, 'sales', 'Lakehouse')                        
        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, lakehouse)
        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, sql_endpoint['database'])        
        
        model = FabricRestApi.get_item_by_name(workspace.id, 'Product Sales DirectLake Model', 'SemanticModel')
        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, lakehouse)
        
        return workspace

    @classmethod
    def deploy_shortcut_solution(cls, target_workspace,
                                 deploy_job = StagingEnvironments.get_dev_environment()):
        """Apply post-deployment updates"""
        AppLogger.log_job(f"Deploying Shortcut Solution to [{target_workspace}] using fabric-cicd")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Shortcut Solution')
        
        staging_folder = FabricRestApi.create_folder(workspace.id ,'staging')
        staging_folder_id = staging_folder.id        
        cls.create_adls_connection_and_variable_library(workspace, staging_folder_id, deploy_job)

        fabric_solution_folder = 'Shortcut Solution'
        FabricCicdManager.deploy(fabric_solution_folder, deploy_job.name, workspace.id)
             
        AppLogger.log_step("Applying post-deploy updates to shortcut solution")
 
        notebook1 = FabricRestApi.get_item_by_name(workspace.id, 'Create 01 Silver Layer', 'Notebook')
        FabricRestApi.run_notebook(workspace.id, notebook1)
                
        notebook2 = FabricRestApi.get_item_by_name(workspace.id, 'Create 02 Gold Layer', 'Notebook')
        FabricRestApi.run_notebook(workspace.id, notebook2)
        
        lakehouse = FabricRestApi.get_item_by_name(workspace.id, 'sales', 'Lakehouse')
        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, lakehouse)
        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, sql_endpoint['database'])
        
        model = FabricRestApi.get_item_by_name(workspace.id, 'Product Sales DirectLake Model', 'SemanticModel')
        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, lakehouse)
        
        return workspace

    @classmethod
    def deploy_pipeline_solution(cls, target_workspace,
                                 deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Data Pipeline Solution using fabric-cicd"""
        AppLogger.log_job(f"Deploying Pipeline Solution to [{target_workspace}] using fabric-cicd")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Pipeline Solution')
        
        staging_folder = FabricRestApi.create_folder(workspace.id ,'staging')
        staging_folder_id = staging_folder.id
        
        cls.create_adls_connection_and_variable_library(workspace, staging_folder_id, deploy_job)
        
        fabric_solution_folder = 'Pipeline Solution'
        FabricCicdManager.deploy(fabric_solution_folder, deploy_job.name, workspace.id)
        
        AppLogger.log_step("Applying post-deploy updates to pipeline solution")
  
        pipeline = FabricRestApi.get_item_by_name(workspace.id, 'Create Lakehouse Tables', 'DataPipeline')
        FabricRestApi.run_data_pipeline(workspace.id, pipeline)
        
        lakehouse = FabricRestApi.get_item_by_name(workspace.id, 'sales', 'Lakehouse')
        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, lakehouse)
        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, sql_endpoint['database'])
        
        model = FabricRestApi.get_item_by_name(workspace.id, 'Product Sales DirectLake Model', 'SemanticModel')
        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, lakehouse)
        
        return workspace

    @classmethod
    def deploy_medallion_solution(cls, target_workspace,
                                  deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Medallion Lakehouse Solution using fabric-cicd"""

        AppLogger.log_job(f"Deploying Medallion Lakehouse Solution to [{target_workspace}] using fabric-cicd")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Medallion Lakehouse Solution')

        staging_folder = FabricRestApi.create_folder(workspace.id, 'staging')
        staging_folder_id = staging_folder.id

        cls.create_adls_connection_and_variable_library(workspace, staging_folder_id, deploy_job)

        fabric_solution_folder = 'Medallion Solution'
        FabricCicdManager.deploy(fabric_solution_folder, deploy_job.name, workspace.id)

        pipeline = FabricRestApi.get_item_by_name(workspace.id, 'Create Lakehouse Tables', 'DataPipeline')
        FabricRestApi.run_data_pipeline(workspace.id, pipeline)
        
        gold_lakehouse = FabricRestApi.get_item_by_name(workspace.id, 'sales', 'Lakehouse')
        gold_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, gold_lakehouse)
        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, gold_sql_endpoint['database'])
        
        model = FabricRestApi.get_item_by_name(workspace.id, 'Product Sales DirectLake Model', 'SemanticModel')
        
        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, gold_lakehouse)
        
        return workspace

    #endregion

    #region Deploy solution using Fabric REST APIs

    @classmethod
    def deploy_powerbi_solution_using_apis(cls, target_workspace, 
                                           deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Power BI Solution using APIs"""

        AppLogger.log_job(f"Deploying Power BI Solution to [{target_workspace}] using Fabric REST APIs")

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace.id, 'Power BI Solution')
        
        create_model_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Power BI Solution',
            'Product Sales Imported Model.SemanticModel'
        )

        DEV_WEB_DATASOURCE_PATH = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Dev/'
        web_datasource_path = deploy_job.parameters[DeploymentJob.web_datasource_path_parameter]

        if web_datasource_path != DEV_WEB_DATASOURCE_PATH:
            model_redirects = { 
                DEV_WEB_DATASOURCE_PATH: web_datasource_path 
            }

            create_model_request['definition'] = ItemDefinitionFactory.update_item_definition_part(
                create_model_request['definition'],
                "definition/expressions.tmdl",
                model_redirects
            )

        model = FabricRestApi.create_item(workspace.id, create_model_request)

        connection = FabricRestApi.create_anonymous_web_connection(web_datasource_path)

        FabricRestApi.bind_semantic_model_to_connection(
            workspace.id,
            model.id,
            connection.id)

        FabricRestApi.refresh_semantic_model(workspace.id, model.id)

        create_report_request = ItemDefinitionFactory.get_create_report_request_from_folder(
            'Power BI Solution',
            'Product Sales Summary.Report',
            model.id)
        
        FabricRestApi.create_item(workspace.id, create_report_request)

        return workspace

    @classmethod
    def deploy_notebook_solution_using_apis(cls, target_workspace,
                                            deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Notebook Solution using APIs"""
        
        AppLogger.log_job(f"Deploying Notebook Solution to [{target_workspace}] using Fabric REST APIs")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Notebook Solution')
        
        cls.create_web_datasource_url_variable_library(workspace, deploy_job)

        lakehouse_name = "sales"
        lakehouse = FabricRestApi.create_lakehouse(workspace.id, lakehouse_name)

        create_notebook_request = ItemDefinitionFactory.get_create_notebook_request_from_folder(
            'Notebook Solution',
            'Create Lakehouse Tables.Notebook',
            workspace.id,
            lakehouse
        )

        notebook = FabricRestApi.create_item(workspace.id, create_notebook_request)

        FabricRestApi.run_notebook(workspace.id, notebook)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, sql_endpoint['database'])

        FabricRestApi.refresh_sql_endpoint_metadata_no_sdk(workspace.id, sql_endpoint['database'])

        create_model_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Notebook Solution',
            'Product Sales DirectLake Model.SemanticModel'
        )

        model_redirects = {
            'abc-xyz.datawarehouse.fabric.microsoft.com': sql_endpoint['server']
        }

        create_model_request = ItemDefinitionFactory.update_part_in_create_request(
            create_model_request,
            'definition/expressions.tmdl',
            model_redirects
        )

        model = FabricRestApi.create_item(workspace.id, create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, lakehouse)

        create_report_request = ItemDefinitionFactory.get_create_report_request_from_folder(
            'Notebook Solution',
            'Product Sales Summary.Report',
            model.id
        )

        FabricRestApi.create_item(workspace.id, create_report_request)

        return workspace

    @classmethod
    def deploy_shortcut_solution_using_apis(cls, target_workspace,
                                            deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Shortcut Solution using APIs"""
        
        AppLogger.log_job(f"Deploying Shortcut Solution to [{target_workspace}] using Fabric REST APIs")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Shortcut Solution')
        
        staging_folder = FabricRestApi.create_folder(workspace.id ,'staging')
        staging_folder_id = staging_folder.id
        
        cls.create_adls_connection_and_variable_library(workspace, staging_folder_id, deploy_job)

        lakehouse_name = "sales"
        lakehouse = FabricRestApi.create_lakehouse(workspace.id, lakehouse_name)

        shortcut_name = "sales-data"
        shortcut_path = "Files"
     
        adls_shortcut_location_variable = "$(/**/environment_settings/adls_server)"
        adls_shortcut_subpath_variable = "$(/**/environment_settings/adls_shortcut_subpath)"
        adls_connection_id_variable = "$(/**/environment_settings/adls_connection_id)"

        FabricRestApi.create_adls_gen2_shortcut(workspace.id,
                                                lakehouse.id,
                                                shortcut_name,
                                                shortcut_path,
                                                adls_shortcut_location_variable,
                                                adls_shortcut_subpath_variable,
                                                adls_connection_id_variable)


        notebook_folders = [
            'staging/Create 01 Silver Layer.Notebook',
            'staging/Create 02 Gold Layer.Notebook'
        ]
        for notebook_folder in notebook_folders:
            create_notebook_request = ItemDefinitionFactory.get_create_notebook_request_from_folder(
                'Shortcut Solution',
                notebook_folder,
                workspace.id,
                lakehouse
            )

            notebook = FabricRestApi.create_item(
                workspace.id, 
                create_notebook_request,
                staging_folder_id
            )
            
            FabricRestApi.run_notebook(workspace.id, notebook)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, sql_endpoint['database'])
        semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'

        create_model_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Shortcut Solution',
            semantic_model_folder
        )

        model_redirects = {
            'abc-xyz.datawarehouse.fabric.microsoft.com': sql_endpoint['server']
        }

        create_model_request = ItemDefinitionFactory.update_part_in_create_request(
            create_model_request,
            'definition/expressions.tmdl',
            model_redirects
        )

        model = FabricRestApi.create_item(workspace.id, create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, lakehouse)
        
        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report'
        ]

        for report_folder in report_folders:
            create_report_request = ItemDefinitionFactory.get_create_report_request_from_folder(
                'Shortcut Solution',
                report_folder,
                model.id
            )

            FabricRestApi.create_item(workspace.id, create_report_request)

        return workspace

    @classmethod
    def deploy_pipeline_solution_using_apis(cls, target_workspace,
                                            deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Data Pipeline Solution using APIs"""
        AppLogger.log_job(f"Deploying Pipeline Solution to [{target_workspace}] using Fabric REST APIs")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Pipeline Solution')
        
        staging_folder = FabricRestApi.create_folder(workspace.id ,'staging')
        staging_folder_id = staging_folder.id
        
        cls.create_adls_connection_and_variable_library(workspace, staging_folder_id, deploy_job)

        lakehouse_name = "sales"
        lakehouse = FabricRestApi.create_lakehouse(workspace.id, lakehouse_name)

        notebook_folders = [
            'staging/Build 01 Silver Layer.Notebook',
            'staging/Build 02 Gold Layer.Notebook'
        ]

        notebook_ids = []
        for notebook_folder in notebook_folders:
            create_notebook_request = \
                ItemDefinitionFactory.get_create_notebook_request_from_folder(
                    'Pipeline Solution',
                    notebook_folder,
                    workspace.id,
                    lakehouse
                )

            notebook = FabricRestApi.create_item(
                workspace.id, 
                create_notebook_request, 
                staging_folder_id)
            
            notebook_ids.append(notebook.id)

        create_pipeline_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Pipeline Solution',
            'staging/Create Lakehouse Tables.DataPipeline'
        )

        pipeline_redirects = {
            '00000000-0000-0000-0000-000000000000': workspace.id,
            '22222222-2222-2222-2222-222222222222': lakehouse.id,
            '33333333-3333-3333-3333-333333333333': notebook_ids[0],
            '44444444-4444-4444-4444-444444444444': notebook_ids[1]
        }
        
        create_pipeline_request = ItemDefinitionFactory.update_part_in_create_request(
            create_pipeline_request,
            'pipeline-content.json',
            pipeline_redirects
        )
        
        pipeline = FabricRestApi.create_item(
            workspace.id, 
            create_pipeline_request,
            staging_folder_id
        )
        
        FabricRestApi.run_data_pipeline(workspace.id, pipeline)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, sql_endpoint['database'])

        create_model_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Pipeline Solution',
            'Product Sales DirectLake Model.SemanticModel'
        )

        model_redirects = {
            'abc-xyz.datawarehouse.fabric.microsoft.com': sql_endpoint['server']
        }

        create_model_request = ItemDefinitionFactory.update_part_in_create_request(
            create_model_request,
            'definition/expressions.tmdl',
            model_redirects
        )

        model = FabricRestApi.create_item(workspace.id, create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, lakehouse)

        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report'
        ]
        for report_folder in report_folders:
            create_report_request = ItemDefinitionFactory.get_create_report_request_from_folder(
                'Pipeline Solution',
                report_folder,
                model.id
            )

            FabricRestApi.create_item(workspace.id, create_report_request)

        return workspace

    @classmethod
    def deploy_medallion_solution_using_apis(cls, target_workspace,
                                             deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Medallion Lakehouse Solution using APIs"""
        
        AppLogger.log_job(f"Deploying Medallion Solution to [{target_workspace}] using Fabric REST APIs")
        workspace = FabricRestApi.create_workspace(target_workspace)
        FabricRestApi.update_workspace_description(workspace.id, 'Medallion Solution')

        staging_folder = FabricRestApi.create_folder(workspace.id, 'staging')
        staging_folder_id = staging_folder.id
        
        cls.create_adls_connection_and_variable_library(workspace, staging_folder_id, deploy_job)

        bronze_lakehouse_name = "sales_bronze"
        silver_lakehouse_name = "sales_silver"
        gold_lakehouse_name = "sales"
     
        bronze_lakehouse = FabricRestApi.create_lakehouse(
            workspace.id,
            bronze_lakehouse_name,
            staging_folder_id
        )

        shortcut_name = "sales-data"
        shortcut_path = "Files"
        adls_shortcut_location_variable = "$(/**/environment_settings/adls_server)"
        adls_shortcut_subpath_variable = "$(/**/environment_settings/adls_shortcut_subpath)"
        adls_connection_id_variable = "$(/**/environment_settings/adls_connection_id)"

        FabricRestApi.create_adls_gen2_shortcut(
            workspace.id,
            bronze_lakehouse.id,
            shortcut_name,
            shortcut_path,
            adls_shortcut_location_variable,
            adls_shortcut_subpath_variable,
            adls_connection_id_variable
        )

        silver_lakehouse = FabricRestApi.create_lakehouse(
            workspace.id,
            silver_lakehouse_name,
            staging_folder_id)
        
        create_notebook_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Medallion Solution', 
                'staging/Build 01 Silver Tables.Notebook'
            )
        
        notebook_redirects = {
            '11111111-1111-1111-1111-111111111111': workspace.id,
            '22222222-2222-2222-2222-222222222222': silver_lakehouse.id,
        }

        create_notebook_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_notebook_request,
                'notebook-content.py', 
                notebook_redirects)

        notebook_build_silver = FabricRestApi.create_item(
            workspace.id, 
            create_notebook_request,
            staging_folder_id)

        FabricRestApi.run_notebook(workspace.id, notebook_build_silver)

        silver_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, silver_lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, silver_sql_endpoint['database'])

        gold_lakehouse = FabricRestApi.create_lakehouse(
            workspace.id,
            gold_lakehouse_name)
        
        create_notebook_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Medallion Solution',
            'staging/Build 02 Gold Tables.Notebook'            
        )
        
        notebook_redirects = {
            '11111111-1111-1111-1111-111111111111': workspace.id,
            '22222222-2222-2222-2222-222222222222': gold_lakehouse.id,
        }

        create_notebook_request = ItemDefinitionFactory.update_part_in_create_request(
            create_notebook_request,
            'notebook-content.py', 
            notebook_redirects
        )

        notebook_build_gold = FabricRestApi.create_item(
            workspace.id, 
            create_notebook_request,
            staging_folder_id)

        FabricRestApi.run_notebook(workspace.id, notebook_build_gold)
        
        gold_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace.id, gold_lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace.id, gold_sql_endpoint['database'])

        create_model_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Medallion Solution',
            'Product Sales DirectLake Model.SemanticModel'
        )
            
        model_redirects = {
            'abc-xyz.datawarehouse.fabric.microsoft.com': gold_sql_endpoint['server']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_model_request,
                'definition/expressions.tmdl',
                model_redirects)

        model = FabricRestApi.create_item(workspace.id, create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model.id, gold_lakehouse)

        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report',
            'Product Sales Top 10 Cities.Report'
        ]
        for report_folder in report_folders:
            
            create_report_request = ItemDefinitionFactory.get_create_report_request_from_folder(
                'Medallion Solution',
                report_folder,
                model.id
            )

            FabricRestApi.create_item(workspace.id, create_report_request)
 
        return workspace

    @classmethod
    def deploy_two_workspace_solution_using_apis(
        cls, project_name,
        deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Two Workspace Medallion Solution using APIs"""
        
        AppLogger.log_job(f"Deploying 2 workspace solution for project [{project_name}] using Fabric REST APIs")
        
        staging_workspace_name = f'{project_name}-staging'        
        staging_workspace = FabricRestApi.create_workspace(staging_workspace_name)
        FabricRestApi.update_workspace_description(staging_workspace.id, 'Medallion Solution - Staging Workspace')
        
        presentation_workspace_name = f'{project_name}-presentation'
        presentation_workspace = FabricRestApi.create_workspace(presentation_workspace_name)
        FabricRestApi.update_workspace_description(presentation_workspace.id, 'Medallion Solution - Presentation Workspace')

        cls.create_adls_connection_and_variable_library(staging_workspace, deploy_job= deploy_job)

        bronze_lakehouse_name = "sales_bronze"
        silver_lakehouse_name = "sales_silver"
        gold_lakehouse_name = "sales"
     
        bronze_lakehouse = FabricRestApi.create_lakehouse(
            staging_workspace.id,
            bronze_lakehouse_name
        )

        shortcut_name = "sales-data"
        shortcut_path = "Files"
        adls_shortcut_location_variable = "$(/**/environment_settings/adls_server)"
        adls_shortcut_subpath_variable = "$(/**/environment_settings/adls_shortcut_subpath)"
        adls_connection_id_variable = "$(/**/environment_settings/adls_connection_id)"

        FabricRestApi.create_adls_gen2_shortcut(
            staging_workspace.id,
            bronze_lakehouse.id,
            shortcut_name,
            shortcut_path,
            adls_shortcut_location_variable,
            adls_shortcut_subpath_variable,
            adls_connection_id_variable
        )

        silver_lakehouse = FabricRestApi.create_lakehouse(
            staging_workspace.id,
            silver_lakehouse_name)
        
        create_notebook_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Medallion Solution', 
                'staging/Build 01 Silver Tables.Notebook'
            )
        
        notebook_redirects = {
            '11111111-1111-1111-1111-111111111111': staging_workspace.id,
            '22222222-2222-2222-2222-222222222222': silver_lakehouse.id,
        }

        create_notebook_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_notebook_request,
                'notebook-content.py', 
                notebook_redirects)

        notebook_build_silver = FabricRestApi.create_item(
            staging_workspace.id, 
            create_notebook_request)

        FabricRestApi.run_notebook(staging_workspace.id, notebook_build_silver)

        silver_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(staging_workspace.id, silver_lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(staging_workspace.id, silver_sql_endpoint['database'])

        gold_lakehouse = FabricRestApi.create_lakehouse(
            presentation_workspace.id,
            gold_lakehouse_name)
        
        create_notebook_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Medallion Solution',
            'staging/Build 02 Gold Tables.Notebook'            
        )
        
        notebook_redirects = {
            '11111111-1111-1111-1111-111111111111': presentation_workspace.id,
            '22222222-2222-2222-2222-222222222222': gold_lakehouse.id,
        }

        create_notebook_request = ItemDefinitionFactory.update_part_in_create_request(
            create_notebook_request,
            'notebook-content.py', 
            notebook_redirects
        )

        notebook_build_gold = FabricRestApi.create_item(
            staging_workspace.id, 
            create_notebook_request)

        FabricRestApi.run_notebook(staging_workspace.id, notebook_build_gold)
        
        gold_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(presentation_workspace.id, gold_lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(presentation_workspace.id, gold_sql_endpoint['database'])

        create_model_request = ItemDefinitionFactory.get_create_item_request_from_folder(
            'Medallion Solution',
            'Product Sales DirectLake Model.SemanticModel'
        )
            
        model_redirects = {
            'abc-xyz.datawarehouse.fabric.microsoft.com': gold_sql_endpoint['server']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_model_request,
                'definition/expressions.tmdl',
                model_redirects)

        model = FabricRestApi.create_item(presentation_workspace.id, create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(presentation_workspace, model.id, gold_lakehouse)

        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report',
            'Product Sales Top 10 Cities.Report'
        ]
        for report_folder in report_folders:
            
            create_report_request = ItemDefinitionFactory.get_create_report_request_from_folder(
                'Medallion Solution',
                report_folder,
                model.id
            )

            FabricRestApi.create_item(presentation_workspace.id, create_report_request)
            
        AdoProjectManager.create_project(project_name, presentation_workspace)
        
        AdoProjectManager.write_file_to_repo(
            project_name, 
            'main', 
            'workspace/staging/ReadMe.md', 
            "hello",
            comment='Adding ReadMe.md to [workspace] folder')
    
        
        FabricRestApi.connect_workspace_to_ado_repo(staging_workspace,project_name, 'main', git_folder='/workspace/staging')

        AdoProjectManager.write_file_to_repo(
            project_name, 
            'main', 
            'workspace/presentation/ReadMe.md', 
            "hello",
            comment='Adding ReadMe.md to [workspace] folder')


        FabricRestApi.connect_workspace_to_ado_repo(presentation_workspace,project_name, 'main', git_folder='/workspace/presentation')
  
 
        return {
            'presentation': presentation_workspace,
            'staging:': staging_workspace
        }


    #endregion

    #region Deployment pipeline support

    @classmethod
    def get_deployment_pipeline_by_name(cls, display_name):
        """Get Deployment Pipeline by Name"""
        for pipeline in FabricRestApi.list_deployment_pipelines():
            if pipeline.display_name == display_name:
                return pipeline
    
        return None

    @classmethod
    def delete_deployment_pipeline_by_name(cls, display_name):
        """Delete Deployment Pipeline by Name"""
        pipeline = FabricRestApi.get_deployment_pipeline_by_name(display_name)
        if pipeline is not None:
            AppLogger.log_step(f"Deleting existing deployment pipeline [{pipeline.display_name}]")
            stages = FabricRestApi.list_deployment_pipeline_stages(pipeline.id)
            for stage in stages:
                if stage.workspace_id is not None:
                    FabricRestApi.unassign_workpace_from_pipeline_stage(pipeline.id, stage.id)

            FabricRestApi.delete_deployment_pipeline(pipeline.id)
            AppLogger.log_substep('Deployment pipeline deleted')

    @classmethod
    def delete_all_deployment_pipelines(cls):
        """Delete All Deployment Pipelines"""
        AppLogger.log_step("Deleting Pipelines")
        for pipeline in FabricRestApi.list_deployment_pipelines():
            AppLogger.log_substep(f"Deleting {pipeline.display_name}")
            stages = FabricRestApi.list_deployment_pipeline_stages(pipeline.id)
            for stage in stages:
                FabricRestApi.unassign_workpace_from_pipeline_stage(pipeline.id, stage.id)

            FabricRestApi.delete_deployment_pipeline(pipeline.id)

    @classmethod
    def deploy_from_dev_to_test(cls, pipeline_name):
        """Deploy Stage from Dev to Test"""
        pipeline = FabricRestApi.get_deployment_pipeline_by_name(pipeline_name)
        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline.id)
        stages_list = list(stages)
        source_stage_id = stages_list[0].id
        target_stage_id = stages_list[1].id

        AppLogger.log_step("Deploy from [dev] to [test]")
        
        AppLogger.log_step(f'source_stage_id: {stages_list[0].id}')
        AppLogger.log_step(f'target_stage_id: {stages_list[1].id}')

        FabricRestApi.deploy_to_pipeline_stage(pipeline.id, source_stage_id, target_stage_id)
        AppLogger.log_substep("Deploy operation complete")

    @classmethod
    def deploy_from_test_to_prod(cls, pipeline_name):
        """Deploy Stage from Dev to Test"""
        pipeline = FabricRestApi.get_deployment_pipeline_by_name(pipeline_name)
        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline.id)
        stages_list = list(stages)
        source_stage_id = stages_list[1].id
        target_stage_id = stages_list[2].id

        AppLogger.log_step("Deploy from [test] to [prod]")
        FabricRestApi.deploy_to_pipeline_stage(pipeline.id, source_stage_id, target_stage_id)
        AppLogger.log_substep("Deploy operation complete")

    @classmethod
    def apply_post_pipeline_deploy_fixes(cls,
                                         workspace_name,
                                         deployment_job: DeploymentJob,
                                         run_etl_jobs = True):
        
        """Apply Post Deploy Fixes"""                    
        AppLogger.log_step(f"Applying post pipeline deploy fixes to [{workspace_name}]")
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        workspace_items = list(FabricRestApi.list_workspace_items(workspace.id))
        
        variable_libraries = list(filter(lambda item: item.type =='VariableLibrary', workspace_items))
        for variable_library in variable_libraries:
            FabricRestApi.set_active_valueset_for_variable_library(
                workspace.id,
                variable_library,
                deployment_job.name
            )

        lakehouses = list(filter(lambda item: item.type=='Lakehouse', workspace_items))
        for lakehouse in lakehouses:
            shortcuts = FabricRestApi.list_shortcuts(workspace.id, lakehouse.id)
            for shortcut in shortcuts:
                if (shortcut.target.type == 'AdlsGen2') and (shortcut.name == 'sales-data'):
                    FabricRestApi.reset_adls_gen2_shortcut(workspace.id, lakehouse.id, shortcut)

        notebooks_list = list(filter(lambda item: item.type=='Notebook', workspace_items))
        sorted_notebooks_list = sorted(notebooks_list, key=lambda notebook: notebook.display_name)
        for notebook in sorted_notebooks_list:
            if run_etl_jobs and 'Create' in notebook.display_name:                
                FabricRestApi.run_notebook(workspace.id, notebook)

        pipelines  = list(filter(lambda item: item.type=='DataPipeline', workspace_items))
        for pipeline in pipelines:
            if run_etl_jobs and 'Create' in pipeline.display_name:
                FabricRestApi.run_data_pipeline(workspace.id, pipeline)

        sql_endpoints =    list(filter(lambda item: item.type=='SQLEndpoint', workspace_items))
        for sql_endpoint in sql_endpoints:
            FabricRestApi.refresh_sql_endpoint_metadata(
                workspace.id,
                sql_endpoint.id)

        models = list(filter(lambda item: item.type=='SemanticModel', workspace_items))
        for model in models:
            if model.display_name == 'Product Sales Imported Model':                
                datasource_path =  deployment_job.parameters[deployment_job.web_datasource_path_parameter]
                DeploymentManager.update_imported_semantic_model_source(
                    workspace,
                    model.display_name,
                    datasource_path)
                FabricRestApi.create_and_bind_semantic_model_connecton(
                    workspace,
                    model.id)

            if model.display_name ==    'Product Sales DirectLake Model':
                target_lakehouse_name = 'sales'
                DeploymentManager.update_directlake_semantic_model_source(
                    workspace_name, 
                    model.display_name,
                    target_lakehouse_name)
                FabricRestApi.create_and_bind_semantic_model_connecton(
                    workspace,
                    model.id)

    #endregion

    #region GIT-sync release process support

    @classmethod
    def apply_post_sync_fixes(cls, workspace_id, deploy_job: DeploymentJob):

        """Apply Post Sync Fixes"""
        workspace = FabricRestApi.get_workspace_info(workspace_id)
        workspace_name = workspace.display_name
        AppLogger.log_step(f"Applying post sync fixes to workspace [{workspace.display_name}]")
        
        workspace_items = list(FabricRestApi.list_workspace_items(workspace.id))

        environment = deploy_job.name
        variable_libraries = list(filter(lambda item: item.type=='VariableLibrary', workspace_items))
        for variable_library in variable_libraries:
            AppLogger.log_substep(f"Set valueSet for variable library [{variable_library.display_name}] to [{environment}]")
            FabricRestApi.set_active_valueset_for_variable_library(workspace.id, variable_library, environment)
            
        lakehouses = list(filter(lambda item: item.type=='Lakehouse', workspace_items))
        for lakehouse in lakehouses:
            shortcuts = FabricRestApi.list_shortcuts(workspace.id, lakehouse.id)
            for shortcut in shortcuts:
                if (shortcut.target.type == 'AdlsGen2') and (shortcut.name == 'sales-data'):
                    FabricRestApi.reset_adls_gen2_shortcut(workspace.id, lakehouse.id, shortcut)

        notebooks = list(filter(lambda item: item.type=='Notebook', workspace_items))
        for notebook in notebooks:
            
            # bind notebooks to sales lakehouse
            if notebook.display_name in [
                'Create Lakehouse Tables', 
                'Build 01 Silver Layer',
                'Build 02 Gold Layer',
                'Create 01 Silver Layer',
                'Create 02 Gold Layer',
                'Build 02 Gold Tables']:
                AppLogger.log_substep(f"Updating notebook [{notebook.display_name}]")
                cls.update_source_lakehouse_in_notebook(
                    workspace_name,
                    notebook.display_name,
                    "sales")

            # bind notebooks to sales_silver lakehouse
            if notebook.display_name in ['Build 01 Silver Tables']:
                AppLogger.log_substep(f"Updating notebook [{notebook.display_name}]")
                cls.update_source_lakehouse_in_notebook(
                    workspace_name,
                    notebook.display_name,
                    "sales_silver")

        models = list(filter(lambda item: item.type=='SemanticModel', workspace_items))
        for model in models:
             
            if model.display_name == 'Product Sales Imported Model':
                web_datasource_path = deploy_job.parameters[DeploymentJob.web_datasource_path_parameter]
                DeploymentManager.update_imported_semantic_model_source(
                    workspace,
                    model.display_name,
                    web_datasource_path)

            if model.display_name == 'Product Sales DirectLake Model':
                AppLogger.log_substep(f"Updating semantic model [{model.display_name}]")
                # fix connection to lakehouse SQL endpoint
                target_lakehouse_name = 'sales'
                DeploymentManager.update_directlake_semantic_model_source(
                    workspace_name, 
                    model.display_name,
                    target_lakehouse_name)

    @classmethod
    def apply_post_deploy_fixes(cls, workspace_id):
        
        """Apply Post Deploy Fixes"""
        workspace = FabricRestApi.get_workspace_info(workspace_id)
        workspace_name = workspace.display_name                         
        workspace_items = list(FabricRestApi.list_workspace_items(workspace.id))
       
        AppLogger.log_step(f"Applying post deploy fixes to [{workspace_name}]")
        
        notebooks = list(filter(lambda item: item.type == 'Notebook', workspace_items))        
        for notebook in notebooks:
            if 'Create' in notebook.display_name:
                FabricRestApi.run_notebook(workspace.id, notebook)
    
        pipelines  = list(filter(lambda item: item.type == 'DataPipeline', workspace_items))
        for pipeline in pipelines:
            if 'Create' in pipeline.display_name:
                FabricRestApi.run_data_pipeline(workspace.id, pipeline)

        sql_endpoints = list(filter(lambda item: item.type == 'SQLEndpoint', workspace_items))
        for sql_endpoint in sql_endpoints:
            FabricRestApi.refresh_sql_endpoint_metadata(
                workspace.id,
                sql_endpoint.id)

        models = list(filter(lambda item: item.type == 'SemanticModel', workspace_items))
        for model in models:
            FabricRestApi.create_and_bind_semantic_model_connecton(
                workspace,
                model.id,
                FabricRestApi.get_item_by_name(workspace.id, 'sales', 'Lakehouse'))
 
    #endregion

    #region fabric-cicd support

    @classmethod
    def generate_yaml_deploy_file(
        cls,
        dev_workspace,
        test_workspace,
        prod_workspace,
        repository_directory = '.',
        item_types_in_scope = None,
        parameter_file = 'parameter.yml'
    ):
        """generate_yaml_deploy_file"""
                
        if item_types_in_scope is None:
            item_types_in_scope = [ 'Lakehouse', 'Notebook', 'DataPipeline', 'SemanticModel', 'Report']

        config = {
            'core': {
                'workspace_id': {
                    'dev': dev_workspace.id,
                    'test': test_workspace.id,
                    'prod': prod_workspace.id
                },
                'repository_directory': {
                    'dev': repository_directory,
                    'test': repository_directory,
                    'prod': repository_directory
                },
                'item_types_in_scope': {
                    'dev': item_types_in_scope.copy(),
                    'test': item_types_in_scope.copy(),
                    'prod': item_types_in_scope.copy()
                },
                'parameter': {
                    'dev': parameter_file,
                    'test': parameter_file,
                    'prod': parameter_file
                },
            }
        }

        yaml_string = yaml.dump(
            config, 
            default_style="'",
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            indent=4
        )

        return yaml_string

    @classmethod
    def generate_yaml_parameter_file(
        cls,
        dev_workspace,
        test_workspace,
        prod_workspace):
        """Generate parameter.yml file"""

        dev_workspace_items = FabricRestApi.list_workspace_items(dev_workspace.id)
        test_workspace_items = FabricRestApi.list_workspace_items(test_workspace.id)
        prod_workspace_items = FabricRestApi.list_workspace_items(prod_workspace.id)

        test_items = {}
        for test_item in test_workspace_items:
            item_name = test_item.display_name + "." + test_item.type
            test_items[item_name] = test_item.id
    
        prod_items = {}
        for prod_item in prod_workspace_items:
            item_name = prod_item.display_name + "." + prod_item.type
            prod_items[item_name] = prod_item.id
            
        find_replace_list = []
        
        find_replace_list.append({
            'find_value':dev_workspace.id, 
            'replace_value': {
             'test': test_workspace.id, 
             'prod': prod_workspace.id
             },
            'item_type': 'Notebook'
        })
          
        for workspace_item in dev_workspace_items:
            
            item_name = workspace_item.display_name + "." + workspace_item.type
            
            if workspace_item.type in ['Lakehouse']:
                            
                find_replace_list.append({
                    'find_value': workspace_item.id, 
                    'replace_value': {
                        'test': test_items[item_name], 
                        'prod': prod_items[item_name]
                    },
                    'item_type': 'Notebook'
                })
    
            if workspace_item.type == 'SemanticModel':

                    dev_datasources = PowerBiRestApi.list_datasources_for_semantic_model(
                        dev_workspace.id,
                        workspace_item.id
                    )

                    test_datasources = PowerBiRestApi.list_datasources_for_semantic_model(
                        test_workspace.id,
                        test_items[item_name]
                    )

                    prod_datasources = PowerBiRestApi.list_datasources_for_semantic_model(
                        prod_workspace.id,
                        prod_items[item_name]
                    )

                    for index, dev_datasource in enumerate(dev_datasources):

                        if dev_datasource['datasourceType'] == "Web":
                            dev_url = dev_datasource['connectionDetails']['url']
                            test_url = test_datasources[index]['connectionDetails']['url']
                            prod_url = prod_datasources[index]['connectionDetails']['url']
                                                            
                            find_replace_list.append({
                                'find_value':dev_url, 
                                'replace_value': {
                                  'test': test_url, 
                                  'prod': prod_url
                                },
                                'item_type': 'SemanticModel'

                            })                  
                            
                        if dev_datasource['datasourceType'] == "Sql":
                            dev_sql_server = dev_datasource['connectionDetails']['server']
                            dev_sql_database = dev_datasource['connectionDetails']['database']
                            test_sql_server = test_datasources[index]['connectionDetails']['server']
                            test_sql_database = test_datasources[index]['connectionDetails']['database']
                            prod_sql_server = prod_datasources[index]['connectionDetails']['server']
                            prod_sql_database = prod_datasources[index]['connectionDetails']['database']
                  
                            find_replace_list.append({
                                'find_value':dev_sql_server, 
                                'replace_value': {
                                  'test': test_sql_server, 
                                  'prod': prod_sql_server
                                },
                                'item_type': 'SemanticModel'
                            })
                            
                            find_replace_list.append({
                                'find_value':dev_sql_database, 
                                'replace_value': {
                                  'test': test_sql_database, 
                                  'prod': prod_sql_database
                                },
                                'item_type': 'SemanticModel'
                            })
                            

        parameter_config = { 
            'find_replace': find_replace_list 
        }
        
        yaml_string = yaml.dump(
            parameter_config,
            default_style="'",
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            indent=4
        )

        return yaml_string

    @classmethod    
    def update_deploy_config_with_workspace_ids(cls,
        yaml_deploy_config,
        dev_workspace_id,
        test_workspace_id,
        prod_workspace_id):
        """Update deploy config with workspace ids"""
        
        yaml_deploy_config['core']['workspace_id']['dev'] = dev_workspace_id
        yaml_deploy_config['core']['workspace_id']['test'] = test_workspace_id    
        yaml_deploy_config['core']['workspace_id']['prod'] = prod_workspace_id
        
        return yaml.safe_dump(
            yaml_deploy_config, 
            sort_keys=False,
            default_style="'",
            default_flow_style=False,
            allow_unicode=True,
            indent=4)

    #endregion

    #region ADO project setup processes

    @classmethod
    def create_feature_workspace_for_ado_project(cls, project_name, feature_name, base_branch):
        """Create feature workspace from new ADO branch"""
             
        # create first feature workspace
        feature_workspace_name = F'{project_name}-dev-{feature_name}'
        feature_branch_name = f'dev-{feature_name}'
        feature_workspace = FabricRestApi.create_workspace(feature_workspace_name)
        AdoProjectManager.create_branch(project_name, feature_branch_name, base_branch)
        FabricRestApi.connect_workspace_to_ado_repo(
            feature_workspace,
            project_name,
            feature_branch_name
        )
        
        DeploymentManager.apply_post_sync_fixes(
            feature_workspace.id,
            StagingEnvironments.get_dev_environment()
        )

        DeploymentManager.apply_post_deploy_fixes(feature_workspace.id)
        
        FabricRestApi.commit_workspace_to_git(
            feature_workspace.id, 
            commit_message=f"Committing feature workspace changes after post-sync fixes."
        )

        FabricRestApi.add_workspace_role_assignment_for_group(
            feature_workspace.id,
            EnvironmentSettings.DEVELOPERS_GROUP_ID,
            "Member"
        )

    @classmethod
    def setup_ado_repo_with_deployment_pipeline(cls, project_name, solution_name, create_feature_workspace = False):
        """Setup Deployment Pipeline"""
        
        pipeline_name = project_name

        # delete existing deployment ppeline with same name if it exists
        cls.delete_deployment_pipeline_by_name(pipeline_name)
        
        dev_workspace_name = f'{project_name}-dev'
        test_workspace_name = f'{project_name}-test'
        prod_workspace_name = project_name
        
        dev_workspace = DeploymentManager.deploy_solution_by_name( 
            solution_name, 
            dev_workspace_name
        )
        
        AdoProjectManager.create_project(project_name)  
        FabricRestApi.connect_workspace_to_ado_repo(dev_workspace, project_name, 'main')

        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)

        variable_group = AdoProjectManager.create_variable_group_for_ado_project(
            'environmental_variables',
            project_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id)
                
        AdoProjectManager.copy_project_files_to_ado_repo(
            project_name,
            'main',
            'ado_setup_using_deployment_pipelines',
            comment='Adding project workflow files',
            variable_group_id=variable_group['id']
        )
        
        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_ado_project(
                project_name,
                'reporting',
                'main'
        )
            
        AppLogger.log_job("Setting up the release process for continuous deployment using deployment pipelines")

        pipeline_stages = [ 'dev', 'test', 'prod' ]
        pipeline = FabricRestApi.create_deployment_pipeline(pipeline_name, pipeline_stages)

        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline.id)

        for stage in stages:
            if stage.order == 0:
                FabricRestApi.assign_workpace_to_pipeline_stage(dev_workspace.id,
                                                                pipeline.id,
                                                                stage.id)
            elif stage.order == 1:
                test_workspace = FabricRestApi.create_workspace(test_workspace_name)
                FabricRestApi.assign_workpace_to_pipeline_stage(test_workspace.id,
                                                                pipeline.id,
                                                                stage.id)
            elif stage.order == 2:
                prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
                FabricRestApi.assign_workpace_to_pipeline_stage(prod_workspace.id,
                                                                pipeline.id,
                                                                stage.id)

        cls.deploy_from_dev_to_test(pipeline_name)

        cls.apply_post_pipeline_deploy_fixes(
            test_workspace_name,
            StagingEnvironments.get_test_environment(),
            run_etl_jobs=True)

        cls.deploy_from_test_to_prod(pipeline_name)

        cls.apply_post_pipeline_deploy_fixes(
            prod_workspace_name,
            StagingEnvironments.get_prod_environment(),
            run_etl_jobs=True)
        
        return dev_workspace

    @classmethod
    def setup_ado_repo_with_git_sync_release_process(cls, project_name, solution_name, create_feature_workspace = False):
        """Setup ADO repo for three branch GitFlow and release process using GIT-sync"""       

        AppLogger.log_job("Configuring GIT integration in Azure DevOps for GIT-sync release process")
               
        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"
    
        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        AdoProjectManager.create_project(project_name)
        AdoProjectManager.create_branch(project_name, 'test', 'main')
        AdoProjectManager.create_branch(project_name, 'dev', 'test')
        AdoProjectManager.set_default_branch(project_name, 'dev')
          
        FabricRestApi.connect_workspace_to_ado_repo(dev_workspace, project_name, 'dev')

        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_ado_project(
                project_name,
                'reporting',
                'dev'
        )
            
        AppLogger.log_job("Setting up the release process for continuous deployment using GIT-sync")
 
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)

        merge_comment = 'Pushing initial set of item definitions from dev to test'
        AdoProjectManager.create_and_merge_pull_request(project_name, 'dev', 'test', merge_comment)

        FabricRestApi.connect_workspace_to_ado_repo(test_workspace, project_name, 'test')
        DeploymentManager.apply_post_sync_fixes(
            test_workspace.id,
            StagingEnvironments.get_test_environment()
        )        
        DeploymentManager.apply_post_deploy_fixes(test_workspace.id)

        merge_comment = 'Pushing initial set of item definitions from test to main'
        AdoProjectManager.create_and_merge_pull_request(project_name, 'test', 'main')
        FabricRestApi.connect_workspace_to_ado_repo(prod_workspace, project_name, 'main')
        DeploymentManager.apply_post_sync_fixes(
            prod_workspace.id, 
            StagingEnvironments.get_prod_environment()
        )
        DeploymentManager.apply_post_deploy_fixes(prod_workspace.id)

        variable_group = AdoProjectManager.create_variable_group_for_ado_project(
            'environmental_variables',
            project_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id)
        
        AdoProjectManager.copy_project_files_to_ado_repo(
            project_name,
            'dev',
            'ado_setup_using_git_sync_release_process',
            comment='Adding project workflow files',
            variable_group_id=variable_group['id'])

        
        merge_comment = 'Pushing GIT workflow files for project from dev to test'
        AdoProjectManager.create_and_merge_pull_request(project_name, 'dev', 'test', merge_comment)
        
        merge_comment = 'Pushing GIT workflow files for project from test to main'
        AdoProjectManager.create_and_merge_pull_request(project_name, 'test', 'main', merge_comment)

    @classmethod
    def setup_ado_repo_with_fabric_cicd_and_gitflow(cls, project_name, solution_name, create_feature_workspace = False):
        """Setup ADO repo for three branch GitFlow and release process using fabric-cicd"""
             
        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"
    
        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        AppLogger.log_job("Setting up the development process for contiguous intergation")
        
        AdoProjectManager.create_project(project_name)
        AdoProjectManager.create_branch(project_name, 'test', 'main')
        AdoProjectManager.create_branch(project_name, 'dev', 'test')
        AdoProjectManager.set_default_branch(project_name, 'dev')
     
        FabricRestApi.connect_workspace_to_ado_repo(dev_workspace, project_name, 'dev')
        
        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_ado_project(
                project_name,
                'reporting',
                'dev'
        )

        AppLogger.log_job("Setting up the release process for continuous deployment using fabric-cicd")
   
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)

        merge_comment = 'Pushing initial set of item definitions from dev to test'    
        AdoProjectManager.create_and_merge_pull_request(project_name, 'dev', 'test', merge_comment)
        
        merge_comment = 'Pushing initial set of item definitions from test to main'
        AdoProjectManager.create_and_merge_pull_request(project_name, 'test', 'main', merge_comment)
        
        AppLogger.log_step("Adding YAML files used in fabric-cicd deployment")
        AppLogger.log_substep("Adding deploy.yml file")
        deploy_config_file_path = f".//templates//FabricSolutions//{solution_name}/deploy.yml"        
        with open(deploy_config_file_path, 'r', encoding='utf-8') as deploy_config_file:
            deploy_config = yaml.safe_load(deploy_config_file)
            deploy_config_yaml = cls.update_deploy_config_with_workspace_ids(
                deploy_config,
                dev_workspace.id,
                test_workspace.id,
                prod_workspace.id
            )
            AdoProjectManager.write_file_to_repo(
                project_name,
                "dev",
                "workspace/deploy.yml",
                deploy_config_yaml,
                "Adding deploy.yml used by fabric_cicd"
        )

        AppLogger.log_substep("Adding parameter.yml file")
        parameter_file_path = f".//templates//FabricSolutions//{solution_name}/parameter.yml"
        with open(parameter_file_path, 'r', encoding='utf-8') as parameter_file:
            parameter_file_content = parameter_file.read()
            AdoProjectManager.write_file_to_repo(
                project_name,
                "dev",
                "workspace/parameter.yml",
                parameter_file_content,
                "Adding parameter.yml used by fabric_cicd"
        )

        merge_comment = 'Pushing fabric-cicd confguration files from dev to test'    
        AdoProjectManager.create_and_merge_pull_request(project_name, 'dev', 'test', merge_comment)

        merge_comment = 'Pushing fabric-cicd confguration files from test to main'    
        AdoProjectManager.create_and_merge_pull_request(project_name, 'test', 'main')

        variable_group = AdoProjectManager.create_variable_group_for_ado_project(
            'environmental_variables',
            project_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id)
                
        AdoProjectManager.copy_project_files_to_ado_repo(
            project_name,
            'dev',
            'ado_setup_with_fabric_cicd_and_gitflow',
            comment='Adding project workflow files',
            variable_group_id=variable_group['id']
        )        
        
        merge_comment = 'Pushing GIT workflow files for project from dev to test'
        AdoProjectManager.create_and_merge_pull_request(project_name, 'dev', 'test', merge_comment)
      
        merge_comment = 'Pushing GIT workflow files for project from test to main'
        AdoProjectManager.create_and_merge_pull_request(project_name, 'test', 'main', merge_comment)
        
        AdoProjectManager.run_pipeline(project_name, 'deploy-from-git-to-workspace', 'test')
        AdoProjectManager.run_pipeline(project_name, 'apply-post-deploy-workspace-updates', 'test')

        AdoProjectManager.run_pipeline(project_name, 'deploy-from-git-to-workspace', 'main')
        AdoProjectManager.run_pipeline(project_name, 'apply-post-deploy-workspace-updates', 'main')

    @classmethod
    def setup_ado_repo_with_fabric_cicd_and_github_flow(cls, project_name, solution_name, create_feature_workspace = False):
        """Set up project with fabric-cicd and GitHub Flow"""

        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"

        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        AppLogger.log_job("Setting up the development process for contiguous intergation")
        
        AdoProjectManager.create_project(project_name)
     
        FabricRestApi.connect_workspace_to_ado_repo(dev_workspace, project_name, 'main')
        
        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_ado_project(
                project_name,
                'reporting',
                'main'
        )

        AppLogger.log_job("Setting up the release process for continuous deployment using fabric-cicd")
   
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
        
        AppLogger.log_step("Adding YAML files used in fabric-cicd deployment")
        AppLogger.log_substep("Adding deploy.yml file")
        deploy_config_file_path = f".//templates//FabricSolutions//{solution_name}/deploy.yml"        
        with open(deploy_config_file_path, 'r', encoding='utf-8') as deploy_config_file:
            deploy_config = yaml.safe_load(deploy_config_file)
            deploy_config_yaml = cls.update_deploy_config_with_workspace_ids(
                deploy_config,
                dev_workspace.id,
                test_workspace.id,
                prod_workspace.id
            )
            AdoProjectManager.write_file_to_repo(
                project_name,
                "main",
                "workspace/deploy.yml",
                deploy_config_yaml,
                "Adding deploy.yml used by fabric-cicd"
        )

        AppLogger.log_substep("Adding parameter.yml file")
        parameter_file_path = f".//templates//FabricSolutions//{solution_name}/parameter.yml"
        with open(parameter_file_path, 'r', encoding='utf-8') as parameter_file:
            parameter_file_content = parameter_file.read()
            AdoProjectManager.write_file_to_repo(
                project_name,
                "main",
                "workspace/parameter.yml",
                parameter_file_content,
                "Adding parameter.yml used by fabric-cicd"
        )
                  
        variable_group = AdoProjectManager.create_variable_group_for_ado_project(
            'environmental_variables',
            project_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id)
        
        AppLogger.log_step("Creating environments for deployment")
        AdoProjectManager.create_environment(project_name, 'test')
        AdoProjectManager.create_environment(project_name, 'prod')
   
        AdoProjectManager.copy_files_from_folder_to_repo(
            project_name,
            'main',
            'ado_setup_with_fabric_cicd_and_github_flow',
            variable_group_id=variable_group['id'])
        
        AppLogger.log_step("Setting pipeline permissions on environments")
        AdoProjectManager.set_pipeline_permission_on_environment(
            project_name, 'test', 'deploy-to-test-workspace')
        AdoProjectManager.set_pipeline_permission_on_environment(
            project_name, 'prod', 'deploy-to-prod-workspace')
        
        time.sleep(10)
        
        AdoProjectManager.run_pipeline(project_name, 'deploy-to-test-workspace', 'main')
        AdoProjectManager.run_pipeline(project_name, 'apply-post-deploy-updates-to-test', 'main')
        AdoProjectManager.run_pipeline(project_name, 'deploy-to-prod-workspace', 'main')
        AdoProjectManager.run_pipeline(project_name, 'apply-post-deploy-updates-to-prod', 'main')
        
        AdoProjectManager.add_approval_to_environment(project_name, 'prod', 'ted@fabricdevcamp.net')   

    @classmethod
    def setup_ado_repo_with_fabric_cicd_and_release_flow(cls, project_name, solution_name, create_feature_workspace = False):
        """Set up project with fabric-cicd and Release Flow"""
             
        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"
    
        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        AppLogger.log_job("Setting up the development process for contiguous intergation")
        
        AdoProjectManager.create_project(project_name)
     
        FabricRestApi.connect_workspace_to_ado_repo(dev_workspace, project_name, 'main')
        
        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_ado_project(
                project_name,
                'reporting',
                'main'
        )

        AppLogger.log_job("Setting up the release process for continuous deployment using fabric-cicd")
   
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
        
        AppLogger.log_step("Adding YAML files used in fabric-cicd deployment")
        AppLogger.log_substep("Adding deploy.yml file")
        deploy_config_file_path = f".//templates//FabricSolutions//{solution_name}/deploy.yml"        
        with open(deploy_config_file_path, 'r', encoding='utf-8') as deploy_config_file:
            deploy_config = yaml.safe_load(deploy_config_file)
            deploy_config_yaml = cls.update_deploy_config_with_workspace_ids(
                deploy_config,
                dev_workspace.id,
                test_workspace.id,
                prod_workspace.id
            )
            AdoProjectManager.write_file_to_repo(
                project_name,
                "main",
                "workspace/deploy.yml",
                deploy_config_yaml,
                "Adding deploy.yml used by fabric-cicd"
        )

        AppLogger.log_substep("Adding parameter.yml file")
        parameter_file_path = f".//templates//FabricSolutions//{solution_name}/parameter.yml"
        with open(parameter_file_path, 'r', encoding='utf-8') as parameter_file:
            parameter_file_content = parameter_file.read()
            AdoProjectManager.write_file_to_repo(
                project_name,
                "main",
                "workspace/parameter.yml",
                parameter_file_content,
                "Adding parameter.yml used by fabric-cicd"
        )

        variable_group = AdoProjectManager.create_variable_group_for_ado_project(
            'environmental_variables',
            project_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id)

        AdoProjectManager.copy_files_from_folder_to_repo(
            project_name,
            'main',
            'ado_setup_with_fabric_cicd_and_release_flow',
            variable_group_id=variable_group['id'])
        
        # AdoProjectManager.copy_project_files_to_ado_repo(
        #     project_name,
        #     'main',
        #     'ado_setup_with_fabric_cicd_and_release_flow',
        #     comment='Adding project workflow files',
        #     variable_group_id=variable_group['id'])
        
        AdoProjectManager.run_pipeline(project_name, 'deploy-to-test-workspace', 'main')
        AdoProjectManager.run_pipeline(project_name, 'apply-post-deploy-updates-to-test', 'main')
        AdoProjectManager.run_pipeline(project_name, 'deploy-to-prod-workspace', 'main')
        AdoProjectManager.run_pipeline(project_name, 'apply-post-deploy-updates-to-prod', 'main')

        # create first test bbuild
        time_now = datetime.now(ZoneInfo("America/New_York"))
        time_now_formatted = time_now.strftime("%Y-%m-%d-%H-%M")
        test_branch_name = str(f'test-{time_now_formatted}')
        AdoProjectManager.create_branch(project_name, test_branch_name, 'main')
        FabricRestApi.update_workspace_description(test_workspace.id, f'BUILD: {test_branch_name}')

        # create first prod build
        prod_branch_name = test_branch_name.replace('test', 'prod')
        AdoProjectManager.create_branch(project_name, prod_branch_name, test_branch_name)
        FabricRestApi.update_workspace_description(prod_workspace.id, f'BUILD: {prod_branch_name}')

    #endregion

    #region GitHub project setup processes

    @classmethod
    def create_feature_workspace_for_github_repo(cls, dev_workspace_name, repo_name, feature_name, base_branch):
        """Create feature workspace from new GitHub branch"""
             
        # create first feature workspace
        feature_workspace_name = F'{dev_workspace_name}-{feature_name}'
        feature_branch_name = f'dev-{feature_name}'
        
        feature_workspace = FabricRestApi.create_workspace(feature_workspace_name)
        FabricRestApi.add_workspace_role_assignment_for_group(
            feature_workspace.id,
            EnvironmentSettings.DEVELOPERS_GROUP_ID,
            "Member"
        )
        GitHubRestApi.create_branch(repo_name, feature_branch_name, base_branch)
        FabricRestApi.connect_workspace_to_github_repo(
            feature_workspace,
            repo_name,
            feature_branch_name
        )
        
        DeploymentManager.apply_post_sync_fixes(
            feature_workspace.id,
            StagingEnvironments.get_dev_environment()
        )

        DeploymentManager.apply_post_deploy_fixes(feature_workspace.id)
        
        FabricRestApi.commit_workspace_to_git(
            feature_workspace.id, 
            commit_message="Committing feature workspace changes after post-sync fixes."
        )

    @classmethod
    def setup_github_repo_with_deployment_pipeline(cls, project_name, solution_name, create_feature_workspace = False):
        """Setup Deployment Pipeline"""
        
        pipeline_name = project_name

        # delete existing deployment ppeline with same name if it exists
        cls.delete_deployment_pipeline_by_name(pipeline_name)
        
        dev_workspace_name = f'{project_name}-dev'
        test_workspace_name = f'{project_name}-test'
        prod_workspace_name = project_name
        
        dev_workspace = DeploymentManager.deploy_solution_by_name( 
            solution_name, 
            dev_workspace_name
        )
        
        repo_name = project_name.replace(' ', '-')
        GitHubRestApi.create_repository(repo_name, dev_workspace)
        
        # add readme.md file in workspace folder
        GitHubRestApi.create_workspace_readme(repo_name, 'main')
  
        FabricRestApi.connect_workspace_to_github_repo(dev_workspace, repo_name, 'main')

        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)

        GitHubRestApi.create_variables_for_github_project(
            repo_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id)
        
        GitHubRestApi.copy_files_from_folder_to_repo(
            repo_name,
            'main',
            'github_setup_using_deployment_pipelines')
        
        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_github_repo(
                dev_workspace_name,
                repo_name,
                'reporting',
                'main'
        )
            
        AppLogger.log_job("Setting up the release process for continuous deployment using deployment pipelines")

        pipeline_stages = [ 'dev', 'test', 'prod' ]
        pipeline = FabricRestApi.create_deployment_pipeline(pipeline_name, pipeline_stages)

        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline.id)

        for stage in stages:
            if stage.order == 0:
                FabricRestApi.assign_workpace_to_pipeline_stage(dev_workspace.id,
                                                                pipeline.id,
                                                                stage.id)
            elif stage.order == 1:
                test_workspace = FabricRestApi.create_workspace(test_workspace_name)
                FabricRestApi.assign_workpace_to_pipeline_stage(test_workspace.id,
                                                                pipeline.id,
                                                                stage.id)
            elif stage.order == 2:
                prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
                FabricRestApi.assign_workpace_to_pipeline_stage(prod_workspace.id,
                                                                pipeline.id,
                                                                stage.id)

        cls.deploy_from_dev_to_test(pipeline_name)

        DeploymentManager.apply_post_pipeline_deploy_fixes(
            test_workspace_name,
            StagingEnvironments.get_test_environment(),
            run_etl_jobs=True)

        DeploymentManager.deploy_from_test_to_prod(pipeline_name)

        DeploymentManager.apply_post_pipeline_deploy_fixes(
            prod_workspace_name,
            StagingEnvironments.get_prod_environment(),
            run_etl_jobs=True)
        
        return dev_workspace

    @classmethod
    def setup_github_repo_with_git_sync_release_process(cls, project_name, solution_name, create_feature_workspace = False):
        """Setup GitHub repo for three branch GitFlow and release process using GIT-sync"""       

        AppLogger.log_job("Configuring GIT integration in Azure DevOps for GIT-sync release process")
               
        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"
    
        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        repo_name = project_name.replace(' ', '-')
        GitHubRestApi.create_repository(repo_name, dev_workspace)
        GitHubRestApi.create_branch(repo_name, 'test', 'main')
        GitHubRestApi.create_branch(repo_name, 'dev', 'test')
        GitHubRestApi.set_default_branch(repo_name, 'dev')
        
        # add readme.md file in workspace folder
        GitHubRestApi.create_workspace_readme(repo_name, 'dev')
          
        FabricRestApi.connect_workspace_to_github_repo(dev_workspace, repo_name, 'dev')

        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_github_repo(
                dev_workspace_name,
                repo_name,
                'reporting',
                'dev'
        )
            
        AppLogger.log_job("Setting up the release process for continuous deployment using GIT-sync")
 
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)

        merge_comment = 'Pushing initial set of item definitions from dev to test'
        GitHubRestApi.create_and_merge_pull_request(
            repo_name,
            'dev', 
            'test',
            merge_comment,
            merge_comment)

        FabricRestApi.connect_workspace_to_github_repo(test_workspace, repo_name, 'test')
        
        DeploymentManager.apply_post_sync_fixes(
            test_workspace.id,
            StagingEnvironments.get_test_environment()
        )
        
        DeploymentManager.apply_post_deploy_fixes(test_workspace.id)

        merge_comment = 'Pushing initial set of item definitions from test to main'
        GitHubRestApi.create_and_merge_pull_request(
            repo_name, 
            'test', 
            'main',
            merge_comment,
            merge_comment)
        
        FabricRestApi.connect_workspace_to_github_repo(prod_workspace, repo_name, 'main')
        
        DeploymentManager.apply_post_sync_fixes(
            prod_workspace.id, 
            StagingEnvironments.get_prod_environment()
        )
        
        DeploymentManager.apply_post_deploy_fixes(prod_workspace.id)
        
        GitHubRestApi.create_variables_for_github_project(
            repo_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id)
        
        GitHubRestApi.copy_files_from_folder_to_repo(
            repo_name,
            'dev',
            'github_setup_using_git_sync_release_process')

        
        merge_comment = 'Pushing GIT workflow files for project from dev to test'
        GitHubRestApi.create_and_merge_pull_request(
            repo_name, 
            'dev', 
            'test', 
            merge_comment,
            merge_comment)
        
        merge_comment = 'Pushing GIT workflow files for project from test to main'
        GitHubRestApi.create_and_merge_pull_request(
            repo_name, 
            'test', 
            'main', 
            merge_comment,
            merge_comment)

    @classmethod
    def setup_github_repo_with_fabric_cicd_and_gitflow(cls, project_name, solution_name, create_feature_workspace = False):
        """Setup GitHub repo for three branch GitFlow and release process using fabric-cicd"""
             
        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"
    
        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        AppLogger.log_job("Setting up the development process for contiguous intergation")
        
        repo_name = project_name.replace(" ", "-")
        GitHubRestApi.create_repository(repo_name, dev_workspace)
        GitHubRestApi.create_branch(repo_name, 'test', 'main')
        GitHubRestApi.create_branch(repo_name, 'dev', 'test')
        GitHubRestApi.set_default_branch(repo_name, 'dev')
        
        # add readme.md file in workspace folder
        GitHubRestApi.create_workspace_readme(repo_name, 'dev')
        
        FabricRestApi.connect_workspace_to_github_repo(dev_workspace, repo_name, 'dev')

        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_github_repo(
                dev_workspace_name,
                repo_name,
                'reporting',
                'dev'
        )

        AppLogger.log_job("Setting up the release process for continuous deployment using fabric-cicd")
   
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)

        merge_comment = 'Pushing initial set of item definitions from dev to test'    
        GitHubRestApi.create_and_merge_pull_request(repo_name, 'dev', 'test', merge_comment, merge_comment)
     
        merge_comment = 'Pushing initial set of item definitions from test to main'
        GitHubRestApi.create_and_merge_pull_request(repo_name, 'test', 'main', merge_comment, merge_comment)
        
        AppLogger.log_step("Adding YAML files used in fabric-cicd deployment")
        AppLogger.log_substep("Adding deploy.yml file")
        deploy_config_file_path = f".//templates//FabricSolutions//{solution_name}/deploy.yml"        
        with open(deploy_config_file_path, 'r', encoding='utf-8') as deploy_config_file:
            deploy_config = yaml.safe_load(deploy_config_file)
            deploy_config_yaml = cls.update_deploy_config_with_workspace_ids(
                deploy_config,
                dev_workspace.id,
                test_workspace.id,
                prod_workspace.id
            )
            GitHubRestApi.write_file_to_repo(
                repo_name,
                "dev",
                "workspace/deploy.yml",
                deploy_config_yaml,
                "Adding deploy.yml used by fabric_cicd"
        )

        AppLogger.log_substep("Adding parameter.yml file")
        parameter_file_path = f".//templates//FabricSolutions//{solution_name}/parameter.yml"
        with open(parameter_file_path, 'r', encoding='utf-8') as parameter_file:
            parameter_file_content = parameter_file.read()
            GitHubRestApi.write_file_to_repo(
                repo_name,
                "dev",
                "workspace/parameter.yml",
                parameter_file_content,
                "Adding parameter.yml used by fabric_cicd"
        )

        merge_comment = 'Pushing fabric-cicd confguration files from dev to test'    
        GitHubRestApi.create_and_merge_pull_request(repo_name, 'dev', 'test', merge_comment, merge_comment)

        merge_comment = 'Pushing fabric-cicd confguration files from test to main'    
        GitHubRestApi.create_and_merge_pull_request(repo_name, 'test', 'main', merge_comment, merge_comment)
        
        GitHubRestApi.create_variables_for_github_project(
            repo_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id
        )

        AppLogger.log_substep("Copying project workflow files to GitHub repo")
        GitHubRestApi.copy_files_from_folder_to_repo(
            repo_name,
            'dev',
            'github_setup_with_fabric_cicd_and_gitflow'
        )
        
        merge_comment = 'Pushing GIT workflow files for project from dev to test'
        GitHubRestApi.create_and_merge_pull_request(
            repo_name,
            'dev', 
            'test', 
            merge_comment,
            merge_comment)
      
        merge_comment = 'Pushing GIT workflow files for project from test to main'
        GitHubRestApi.create_and_merge_pull_request(
            repo_name,
            'test', 
            'main', 
            merge_comment,
            merge_comment)
        
        GitHubRestApi.run_workflow(repo_name, 'deploy-from-git-to-workspace.yml', 'test')
        GitHubRestApi.run_workflow(repo_name, 'apply-post-deploy-workspace-updates.yml', 'test')

        GitHubRestApi.run_workflow(repo_name, 'deploy-from-git-to-workspace.yml', 'main')
        GitHubRestApi.run_workflow(repo_name, 'apply-post-deploy-workspace-updates.yml', 'main')

    @classmethod
    def setup_github_repo_with_fabric_cicd_and_github_flow(cls, project_name, solution_name, create_feature_workspace = False):
        """Set up GitHub repo with fabric-cicd and GitHub Flow"""
             
        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"
    
        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        AppLogger.log_job("Setting up the development process for contiguous intergation")
        
        repo_name = project_name.replace(' ','-')
        GitHubRestApi.create_repository(repo_name, dev_workspace)
                     
        FabricRestApi.connect_workspace_to_github_repo(
            dev_workspace,
            repo_name,
            'main'
        )        
   
        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_github_repo(
                dev_workspace_name,
                repo_name,
                'reporting',
                'main'
        )

        AppLogger.log_job("Setting up the release process for continuous deployment using fabric-cicd")
   
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
        
        AppLogger.log_step("Adding YAML files used in fabric-cicd deployment")
        AppLogger.log_substep("Adding deploy.yml file")
        deploy_config_file_path = f".//templates//FabricSolutions//{solution_name}/deploy.yml"        
        with open(deploy_config_file_path, 'r', encoding='utf-8') as deploy_config_file:
            deploy_config = yaml.safe_load(deploy_config_file)
            deploy_config_yaml = cls.update_deploy_config_with_workspace_ids(
                deploy_config,
                dev_workspace.id,
                test_workspace.id,
                prod_workspace.id
            )
            GitHubRestApi.write_file_to_repo(
                repo_name,
                "main",
                "workspace/deploy.yml",
                deploy_config_yaml,
                "Adding deploy.yml used by fabric-cicd"
        )

        AppLogger.log_substep("Adding parameter.yml file")
        parameter_file_path = f".//templates//FabricSolutions//{solution_name}/parameter.yml"
        with open(parameter_file_path, 'r', encoding='utf-8') as parameter_file:
            parameter_file_content = parameter_file.read()
            GitHubRestApi.write_file_to_repo(
                repo_name,
                "main",
                "workspace/parameter.yml",
                parameter_file_content,
                "Adding parameter.yml used by fabric-cicd"
        )
            
        GitHubRestApi.create_environment(repo_name, 'dev')
        GitHubRestApi.create_environment(repo_name, 'test')
        GitHubRestApi.create_environment(repo_name, 'prod')
            
        GitHubRestApi.create_variables_for_github_project(
            repo_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id
        )

        AppLogger.log_step("Coppying workflow project files to repo")              
        GitHubRestApi.copy_files_from_folder_to_repo(
            repo_name,
            'main',
            'github_setup_with_fabric_cicd_and_github_flow'
        )
        
        GitHubRestApi.run_workflow(repo_name, 'deploy-to-test-workspace.yml', 'main')
        GitHubRestApi.run_workflow(repo_name, 'apply-post-deploy-updates-to-test.yml', 'main')

        GitHubRestApi.run_workflow(repo_name, 'deploy-to-prod-workspace.yml', 'main')
        GitHubRestApi.run_workflow(repo_name, 'apply-post-deploy-updates-to-prod.yml', 'main')
        
        # add protection rule for main branch to require all updates made through pull requests        
        GitHubRestApi.add_protection_ruleset_for_branch(repo_name, 'main')    
        
        # update prod environment to require approval for deployment    
        GitHubRestApi.create_environment(repo_name, 'prod', add_reviewers=True)

    @classmethod
    def setup_github_repo_with_fabric_cicd_and_release_flow(cls, project_name, solution_name, create_feature_workspace = False):
        """Set up GitHub repo with fabric-cicd and Release Flow"""
             
        dev_workspace_name = f"{project_name}-dev"
        test_workspace_name = f"{project_name}-test"
        prod_workspace_name = f"{project_name}"
    
        dev_workspace = DeploymentManager.deploy_solution_by_name(
            solution_name,
            dev_workspace_name
        )
        
        AppLogger.log_job("Setting up the development process for contiguous intergation")
        
        repo_name = project_name.replace(' ','-')
        GitHubRestApi.create_repository(repo_name, dev_workspace)
        
        GitHubRestApi.create_workspace_readme(repo_name, 'main')
             
        FabricRestApi.connect_workspace_to_github_repo(
            dev_workspace,
            repo_name,
            'main'
        )
        
        if create_feature_workspace:
            AppLogger.log_job("Creating new feature workspace for report development")
            DeploymentManager.create_feature_workspace_for_github_repo(
                dev_workspace_name,
                repo_name,
                'reporting',
                'main'
        )

        AppLogger.log_job("Setting up the release process for continuous deployment using fabric-cicd")
   
        test_workspace = FabricRestApi.create_workspace(test_workspace_name)
        prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
        
        AppLogger.log_step("Adding YAML files used in fabric-cicd deployment")
        AppLogger.log_substep("Adding deploy.yml file")
        deploy_config_file_path = f".//templates//FabricSolutions//{solution_name}/deploy.yml"        
        with open(deploy_config_file_path, 'r', encoding='utf-8') as deploy_config_file:
            deploy_config = yaml.safe_load(deploy_config_file)
            deploy_config_yaml = cls.update_deploy_config_with_workspace_ids(
                deploy_config,
                dev_workspace.id,
                test_workspace.id,
                prod_workspace.id
            )
            GitHubRestApi.write_file_to_repo(
                repo_name,
                "main",
                "workspace/deploy.yml",
                deploy_config_yaml,
                "Adding deploy.yml used by fabric-cicd"
        )

        AppLogger.log_substep("Adding parameter.yml file")
        parameter_file_path = f".//templates//FabricSolutions//{solution_name}/parameter.yml"
        with open(parameter_file_path, 'r', encoding='utf-8') as parameter_file:
            parameter_file_content = parameter_file.read()
            GitHubRestApi.write_file_to_repo(
                repo_name,
                "main",
                "workspace/parameter.yml",
                parameter_file_content,
                "Adding parameter.yml used by fabric-cicd"
        )
            
        GitHubRestApi.create_variables_for_github_project(
            repo_name,
            dev_workspace.id,
            test_workspace.id,
            prod_workspace.id
        )

        AppLogger.log_step("Coppying workflow project files to repo")              
        GitHubRestApi.copy_files_from_folder_to_repo(
            repo_name,
            'main',
            'github_setup_with_fabric_cicd_and_release_flow'
        )
        
        GitHubRestApi.run_workflow(repo_name, 'deploy-to-test-workspace.yml', 'main')
        GitHubRestApi.run_workflow(repo_name, 'apply-post-deploy-updates-to-test.yml', 'main')

        GitHubRestApi.run_workflow(repo_name, 'deploy-to-prod-workspace.yml', 'main')
        GitHubRestApi.run_workflow(repo_name, 'apply-post-deploy-updates-to-prod.yml', 'main')

        # create first test bbuild
        time_now = datetime.now(ZoneInfo("America/New_York"))
        time_now_formatted = time_now.strftime("%Y-%m-%d-%H-%M")
        test_branch_name = str(f'test-{time_now_formatted}')
        GitHubRestApi.create_branch(repo_name, test_branch_name, 'main')
        FabricRestApi.update_workspace_description(test_workspace.id, f'BUILD: {test_branch_name}')

        # create first prod build
        prod_branch_name = test_branch_name.replace('test', 'prod')
        GitHubRestApi.create_branch(repo_name, prod_branch_name, test_branch_name)
        FabricRestApi.update_workspace_description(prod_workspace.id, f'BUILD: {prod_branch_name}')

    #endregion
    
    @classmethod
    def setup_ado_repo_for_terraform(cls, project_name):
        """Set up ADO project with variables/secrets"""
              
        AppLogger.log_job("Setting up ADO repo")
        
        AdoProjectManager.create_project(project_name)
     
        variable_group = AdoProjectManager.create_variable_group_for_ado_project(
            'environmental_variables',
            project_name,
            '11111111-1111-1111-1111-111111111111',
            '11111111-1111-1111-1111-111111111112',
            '11111111-1111-1111-1111-111111111113')
        
        AppLogger.log_step("Creating environments for deployment")
        AdoProjectManager.create_environment(project_name, 'test')
        AdoProjectManager.create_environment(project_name, 'prod')
   
        AdoProjectManager.copy_files_from_folder_to_repo(
            project_name,
            'main',
            'ado_setup_with_terraform',
            variable_group_id=variable_group['id'])
        
