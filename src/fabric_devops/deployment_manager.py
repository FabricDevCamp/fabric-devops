"""Deployment Manager"""

import base64
import json

from .app_logger import AppLogger
#from .app_settings import EnvironmentSettings
from .deployment_job import DeploymentJob
from .fabric_rest_api import FabricRestApi
from .item_definition_factory import ItemDefinitionFactory
from .variable_library import VariableLibrary, Valueset
from .staging_environments import StagingEnvironments
from .github_rest_api import GitHubRestApi
from .ado_project_manager import AdoProjectManager

class DeploymentManager:
    """Deployment Manager"""

    @classmethod
    def deploy_solution_by_name(cls, 
                                solution_name, 
                                target_workspace = None,
                                deploy_job = StagingEnvironments.get_dev_environment() ):
        """Deploy Solution by Name"""

        if target_workspace is None:
            target_workspace = solution_name

        workspace = None

        match solution_name:
            case 'Custom Power BI Solution':
                workspace = cls.deploy_powerbi_solution(target_workspace, deploy_job)
            case 'Custom Notebook Solution':
                workspace = cls.deploy_notebook_solution(target_workspace, deploy_job)
            case 'Custom Shortcut Solution':
                workspace = cls.deploy_shortcut_solution(target_workspace, deploy_job)
            case 'Custom Data Pipeline Solution':
                workspace = cls.deploy_data_pipeline_solution(target_workspace, deploy_job)
            case 'Custom CopyJob Solution':
                workspace = cls.deploy_copyjob_solution(target_workspace, deploy_job)
            case 'Custom User Data Function Solution':
                workspace = cls.deploy_udf_solution(target_workspace, deploy_job)
            case 'Custom Dataflow Gen2 Solution':
                workspace = cls.deploy_dataflow_solution(target_workspace, deploy_job)
            case 'Custom Warehouse Solution':
                workspace = cls.deploy_warehouse_solution(target_workspace, deploy_job)
            case 'Custom Realtime Solution':
                workspace = cls.deploy_realtime_solution(target_workspace)
            case 'Custom Medallion Warehouse Solution':
                workspace = cls.deploy_medallion_warehouse_solution(target_workspace, deploy_job)
            case 'Custom Notebook Solution with Variable Library':
                workspace = cls.deploy_notebook_solution_with_varlib(target_workspace, deploy_job)
            case "Custom Shortcut Solution with Variable Library":
                workspace = cls.deploy_shortcut_solution_with_varlib(target_workspace, deploy_job)
            case 'Custom Data Pipeline Solution with Variable Library':
                workspace = cls.deploy_data_pipeline_solution_with_varlib(target_workspace, deploy_job)
        
        if workspace is None:
            raise LookupError(f'Unknown solution name [{solution_name}]')

        return workspace

    @classmethod
    def deploy_powerbi_solution(cls,
                                target_workspace, 
                                deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Power BI Solution with Parameters"""

        AppLogger.log_job(f"Deploying Customer Power BI Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters('web')

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom Power BI Solution')

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Product Sales Imported Model.SemanticModel')

        web_datasource_path = deploy_job.parameters[DeploymentJob.web_datasource_path_parameter]

        model_redirects = { '{WEB_DATASOURCE_PATH}': web_datasource_path }

        create_model_request['definition'] = \
        ItemDefinitionFactory.update_item_definition_part(
            create_model_request['definition'],
            "definition/expressions.tmdl",
            model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        connection = FabricRestApi.create_anonymous_web_connection(web_datasource_path, workspace)

        FabricRestApi.bind_semantic_model_to_connection(
            workspace['id'],
            model['id'],
            connection['id'])

        FabricRestApi.refresh_semantic_model(workspace['id'], model['id'])

        create_report_request = \
            ItemDefinitionFactory.get_create_report_request_from_folder(
                'Product Sales Summary.Report',
                model['id'])
        
        FabricRestApi.create_item(workspace['id'], create_report_request)

        return workspace

    @classmethod
    def deploy_notebook_solution(cls,
                                 target_workspace,
                                 deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Notebook Solution"""

        lakehouse_name = "sales"

        AppLogger.log_job(f"Deploying Custom Notebook Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters('web')

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom Notebook Solution')

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        create_notebook_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Create Lakehouse Tables.Notebook')

        notebook_redirects = {
            '{WORKSPACE_ID}': workspace['id'],
            '{LAKEHOUSE_ID}': lakehouse['id'],
            '{LAKEHOUSE_NAME}': lakehouse['displayName'],
            '{WEB_DATASOURCE_PATH}': deploy_job.parameters[deploy_job.web_datasource_path_parameter]
        }

        create_notebook_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_notebook_request,
                                                                'notebook-content.py', 
                                                                notebook_redirects)

        notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)

        FabricRestApi.run_notebook(workspace['id'], notebook)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Product Sales DirectLake Model.SemanticModel')

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
            '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_model_request,
                                                                'definition/expressions.tmdl',
                                                                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        create_report_request = \
            ItemDefinitionFactory.get_create_report_request_from_folder(
                'Product Sales Summary.Report',
                model['id'])

        FabricRestApi.create_item(workspace['id'], create_report_request)

        return workspace

    @classmethod
    def deploy_shortcut_solution(cls,
                                 target_workspace,
                                 deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Shortcut Solution"""

        lakehouse_name = "sales"
        notebook_folders = [
            'Create 01 Silver Layer.Notebook',
            'Create 02 Gold Layer.Notebook'
        ]
        semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report'
        ]

        AppLogger.log_job(f"Deploying Custom Shortcut Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters('adls')

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom Shortcut Solution')

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
        adls_path = f'/{adls_container_name}{adls_container_path}'

        connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            adls_server,
            adls_path,
            workspace)

        shortcut_name = "sales-data"
        shortcut_path = "Files"
        shortcut_location = adls_server
        shortcut_subpath = adls_path

        FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                                lakehouse['id'],
                                                shortcut_name,
                                                shortcut_path,
                                                shortcut_location,
                                                shortcut_subpath,
                                                connection['id'])

        for notebook_folder in notebook_folders:
            create_notebook_request = \
                ItemDefinitionFactory.get_create_notebook_request_from_folder(
                    notebook_folder,
                    workspace['id'],
                    lakehouse)

            notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
            FabricRestApi.run_notebook(workspace['id'], notebook)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                semantic_model_folder)

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
            '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_model_request,
                                                                'definition/expressions.tmdl',
                                                                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        for report_folder in report_folders:
            create_report_request = \
                ItemDefinitionFactory.get_create_report_request_from_folder(
                    report_folder,
                    model['id'])

            FabricRestApi.create_item(workspace['id'], create_report_request)

        return workspace

    @classmethod
    def deploy_data_pipeline_solution(cls,
                                        target_workspace,
                                        deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Data Pipeline Solution"""

        lakehouse_name = "sales"
        notebook_folders = [
            'Build 01 Silver Layer.Notebook',
            'Build 02 Gold Layer.Notebook'
        ]
        data_pipeline_folder = 'Create Lakehouse Tables.DataPipeline'
        semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report',
            'Product Sales Top 10 Cities.Report'
        ]

        AppLogger.log_job(f"Deploying Custom Data Pipeline Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters('adls')

        workspace = FabricRestApi.create_workspace(target_workspace)

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        notebook_ids = []

        for notebook_folder in notebook_folders:
            create_notebook_request = \
                ItemDefinitionFactory.get_create_notebook_request_from_folder(
                    notebook_folder,
                    workspace['id'],
                    lakehouse)

            notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
            notebook_ids.append(notebook['id'])

        adls_server_path = deploy_job.parameters[DeploymentJob.adls_server_parameter]
        adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_path = adls_container_name + adls_container_path

        connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            adls_server_path,
            adls_path,
            workspace)

        create_pipeline_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                data_pipeline_folder)

        pipeline_redirects = {
            '{WORKSPACE_ID}': workspace['id'],
            '{LAKEHOUSE_ID}': lakehouse['id'],
            '{CONNECTION_ID}': connection['id'],
            '{CONTAINER_NAME}': adls_container_name,
            '{CONTAINER_PATH}': adls_container_path,
            '{NOTEBOOK_ID_BUILD_SILVER}': notebook_ids[0],
            '{NOTEBOOK_ID_BUILD_GOLD}': notebook_ids[1]
        }

        create_pipeline_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_pipeline_request,
                'pipeline-content.json',
                pipeline_redirects)

        pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)

        FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                semantic_model_folder)

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
            '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_model_request,
                'definition/expressions.tmdl',
                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        for report_folder in report_folders:
            
            create_report_request = \
                ItemDefinitionFactory.get_create_report_request_from_folder(
                    report_folder,
                    model['id'])

            FabricRestApi.create_item(workspace['id'], create_report_request)
 
        return workspace

    @classmethod
    def deploy_copyjob_solution(cls,
                                        target_workspace,
                                        deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy CopyJob Solution"""

        lakehouse_name = "sales"
             
        AppLogger.log_job(f"Deploying Custom Data Pipeline Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters('adls')

        workspace = FabricRestApi.create_workspace(target_workspace)

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name, enable_schemas=True)


        create_notebook_request = \
                ItemDefinitionFactory.get_create_item_request_from_folder(
                    'Create Lakehouse Schemas.Notebook')

        notebook_redirects = {
            '{WORKSPACE_ID}': workspace['id'],
            '{LAKEHOUSE_ID}': lakehouse['id'],
            '{LAKEHOUSE_NAME}': lakehouse['displayName']
        }

        create_notebook_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_notebook_request,
                                                                'notebook-content.sql', 
                                                                notebook_redirects)

        notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)

        FabricRestApi.run_notebook(workspace['id'], notebook)
        
        adls_server_path = deploy_job.parameters[DeploymentJob.adls_server_parameter]
        adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_path = adls_container_name + adls_container_path

        connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            adls_server_path,
            adls_path,
            workspace)
        
        ingest_copyjob_create_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Ingest Sales Data CSV File.CopyJob')

        copyjob_redirects = {
            '{WORKSPACE_ID}': workspace['id'],
            '{LAKEHOUSE_ID}': lakehouse['id'],
            '{CONNECTION_ID}': connection['id']
        }

        ingest_copyjob_create_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                ingest_copyjob_create_request,
                'copyjob-content.json',
                copyjob_redirects)

        ingest_copyjob = FabricRestApi.create_item(workspace['id'], ingest_copyjob_create_request)

        FabricRestApi.run_copyjob(workspace['id'], ingest_copyjob)        

        load_copyjob_folders = [
            'Load Silver Customers Table.CopyJob',
            'Load Silver InvoiceDetails Table.CopyJob',
            'Load Silver Invoices Table.CopyJob',
            'Load Silver Products Table.CopyJob'
        ]

        for load_copyjob_folder in load_copyjob_folders:

            load_copyjob_create_request = \
                ItemDefinitionFactory.get_create_item_request_from_folder(
                    load_copyjob_folder)

            load_copyjob_redirects = {
                '{WORKSPACE_ID}': workspace['id'],
                '{LAKEHOUSE_ID}': lakehouse['id'],
            }

            copyjob_create_request = \
                ItemDefinitionFactory.update_part_in_create_request(
                    load_copyjob_create_request,
                    'copyjob-content.json',
                    load_copyjob_redirects)

            load_copyjob = FabricRestApi.create_item(workspace['id'], copyjob_create_request)

            FabricRestApi.run_copyjob(workspace['id'], load_copyjob)


        create_mv_notebook_request = \
                ItemDefinitionFactory.get_create_item_request_from_folder(
                    'Create Lakehouse Materialized Views.Notebook')

        mv_notebook_redirects = {
            '{WORKSPACE_ID}': workspace['id'],
            '{LAKEHOUSE_ID}': lakehouse['id'],
            '{LAKEHOUSE_NAME}': lakehouse['displayName']
        }

        create_notebook_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_mv_notebook_request,
                                                                'notebook-content.sql', 
                                                                mv_notebook_redirects)

        mv_notebook = FabricRestApi.create_item(workspace['id'], create_mv_notebook_request)

        FabricRestApi.run_notebook(workspace['id'], mv_notebook)


        #FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], lakehouse['id'])

        # sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

        # FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        # create_model_request = \
        #     ItemDefinitionFactory.get_create_item_request_from_folder(
        #         semantic_model_folder)

        # model_redirects = {
        #     '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
        #     '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        # }

        # create_model_request = \
        #     ItemDefinitionFactory.update_part_in_create_request(
        #         create_model_request,
        #         'definition/expressions.tmdl',
        #         model_redirects)

        # model = FabricRestApi.create_item(workspace['id'], create_model_request)

        # FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        # for report_folder in report_folders:
            
        #     create_report_request = \
        #         ItemDefinitionFactory.get_create_report_request_from_folder(
        #             report_folder,
        #             model['id'])

        #     FabricRestApi.create_item(workspace['id'], create_report_request)
 
        # return workspace



    @classmethod
    def deploy_udf_solution(cls,
                             target_workspace,
                             deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy User Data Function Solution"""

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom User Data Function Solution')

        lakehouse_name = 'sales'
        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        udf_set_folders = [
            'udf_playground.UserDataFunction',
            'sales_data_transforms.UserDataFunction'
        ]

        notebook_folders = [
            'udf_playground_test.Notebook',
            'Create Lakehouse Tables with UDFs.Notebook'
        ]
      

        for udf_set_folder in udf_set_folders:    
            udf_create_request = ItemDefinitionFactory.get_create_item_request_from_folder(
                udf_set_folder
            )

            udf_redirects = {
                '{WORKSPACE_ID}': workspace['id'],
                '{LAKEHOUSE_ID}': lakehouse['id']
            }

            create_udf_create_request  = \
                ItemDefinitionFactory.update_part_in_create_request(udf_create_request,
                                                                    'definition.json', 
                                                                    udf_redirects)


            FabricRestApi.create_item(workspace['id'], create_udf_create_request)

        for notebook_folder in notebook_folders:
            
            create_notebook_request = \
                ItemDefinitionFactory.get_create_notebook_request_from_folder(
                    notebook_folder,
                    workspace['id'],
                    lakehouse)
            
            notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
            
            FabricRestApi.run_notebook(workspace['id'], notebook)

        return workspace

    @classmethod
    def deploy_dataflow_solution(cls,
                                 target_workspace,
                                 deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Dataflow Gen2 Solution"""

   
        # semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
        # report_folders = [
        #     'Product Sales Summary.Report',
        #     'Product Sales Time Intelligence.Report'
        # ]

        AppLogger.log_job(f"Deploying Custom Dataflow Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters('web')

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom Dataflow Solution')

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], "sales")

        web_datasource_path = deploy_job.parameters[deploy_job.web_datasource_path_parameter]
        connection = FabricRestApi.create_anonymous_web_connection(web_datasource_path, workspace)

        create_dataflow_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'TestDataflow.Dataflow')

        dataflow_redirects = {
            '{CONNECTION_ID}': connection['id'],
            '{LAKEHOUSE_ID}': lakehouse['id']
        }

        create_dataflow_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_dataflow_request,
                                                                'queryMetadata.json',
                                                                dataflow_redirects)
        

        dataflow = FabricRestApi.create_item(workspace['id'], create_dataflow_request)

        FabricRestApi.apply_changes_to_dataflow(workspace['id'], dataflow)

        FabricRestApi.run_dataflow(workspace['id'], dataflow)

        print (json.dumps(dataflow, indent=4))

        
        # create_model_request = \
        #     ItemDefinitionFactory.get_create_item_request_from_folder(
        #         semantic_model_folder)

        # model_redirects = {
        #     '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
        #     '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        # }

        # create_model_request = \
        #     ItemDefinitionFactory.update_part_in_create_request(create_model_request,
        #                                                         'definition/expressions.tmdl',
        #                                                         model_redirects)

        # model = FabricRestApi.create_item(workspace['id'], create_model_request)

        # FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        # for report_folder in report_folders:
        #     create_report_request = \
        #         ItemDefinitionFactory.get_create_report_request_from_folder(
        #             report_folder,
        #             model['id'])

        #     FabricRestApi.create_item(workspace['id'], create_report_request)

        return workspace

    @classmethod
    def deploy_warehouse_solution(
        cls,
        target_workspace,
        deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Warehouse Solution"""
        
        lakehouse_name = "staging"
        warehouse_name = "sales"

        data_pipelines = [
            { 'name': 'Load Tables in Staging Lakehouse', 'template':'LoadTablesInStagingLakehouseWithParams.json'},
            { 'name': 'Create Warehouse Tables', 'template':'CreateWarehouseTables.json'},
            { 'name': 'Create Warehouse Stored Procedures', 'template':'CreateWarehouseStoredProcedures.json'},
            { 'name': 'Refresh Warehouse Tables', 'template':'RefreshWarehouseTables.json'}
        ]

        semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report',
            'Product Sales Top 10 Cities.Report'
        ]


        AppLogger.log_job("Deploying Warehouse solution")

        deploy_job.display_deployment_parameters('adls')

        workspace = FabricRestApi.create_workspace(target_workspace)

        data_prep_folder = FabricRestApi.create_folder(workspace['id'], 'data_prep')
        data_prep_folder_id = data_prep_folder['id']

        lakehouse = FabricRestApi.create_lakehouse(
            workspace['id'],
            lakehouse_name,
            data_prep_folder_id)

        warehouse = FabricRestApi.create_warehouse(
            workspace['id'],
            warehouse_name
        )

        warehouse_connect_string = FabricRestApi.get_warehouse_connection_string(
                workspace['id'],
                warehouse['id'])
        
        adls_server_path = deploy_job.parameters[DeploymentJob.adls_server_parameter]
        adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_path = adls_container_name + adls_container_path

        connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            adls_server_path,
            adls_path,
            workspace)

        for data_pipeline in data_pipelines:
            template_file = f"DataPipelines//{data_pipeline['template']}"
            template_content = ItemDefinitionFactory.get_template_file(template_file)
            template_content = template_content.replace('{WORKSPACE_ID}', workspace['id']) \
                                                 .replace('{LAKEHOUSE_ID}', lakehouse['id']) \
                                                 .replace('{WAREHOUSE_ID}', warehouse['id']) \
                                                 .replace('{WAREHOUSE_CONNECT_STRING}', warehouse_connect_string) \
                                                 .replace('{CONNECTION_ID}', connection['id']) \
                                                 .replace('{CONTAINER_NAME}', adls_container_name) \
                                                 .replace('{CONTAINER_PATH}', adls_container_path)                                                         

            pipeline_create_request = ItemDefinitionFactory.get_data_pipeline_create_request(
                data_pipeline['name'],
                template_content)
            
            pipeline = FabricRestApi.create_item(
                workspace['id'], 
                pipeline_create_request,
                data_prep_folder_id)

            FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

        # FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                semantic_model_folder)

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': warehouse_connect_string,
            '{SQL_ENDPOINT_DATABASE}': warehouse['id']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_model_request,
                'definition/expressions.tmdl',
                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        for report_folder in report_folders:
            
            create_report_request = \
                ItemDefinitionFactory.get_create_report_request_from_folder(
                    report_folder,
                    model['id'])

            FabricRestApi.create_item(workspace['id'], create_report_request)
 
        return workspace

    @classmethod
    def deploy_realtime_solution(cls, target_workspace):
        """Deploy Real Time Solution"""
        
        workspace_name = target_workspace

        AppLogger.log_job(f'Deploying {workspace_name}')

        eventhouse_name = "Rental Bikes"
        kql_database_name = "Rental Bike Events"
        kql_queryset_name = "Rental Bike Queries"
        eventstream_name = "rental_bike_event_data"
        realtime_dashboard_name = "Rental Bike Dashboard"
        semantic_model_name = 'Rental Bike Event Model'
        report_name = 'Rental Bike Locations Report'

        workspace = FabricRestApi.create_workspace(workspace_name)

        create_eventhouse_request = \
            ItemDefinitionFactory.get_eventhouse_create_request(eventhouse_name)
        
        eventhouse_item = FabricRestApi.create_item(workspace['id'], create_eventhouse_request)

        eventhouse = FabricRestApi.get_eventhouse(workspace['id'], eventhouse_item['id'])

        query_service_uri = eventhouse['properties']['queryServiceUri']

        create_kql_database_request = \
            ItemDefinitionFactory.get_kql_database_create_request(kql_database_name, 
                                                                eventhouse)
        
        kql_database = FabricRestApi.create_item(workspace['id'], create_kql_database_request)

        create_eventstream_request = \
            ItemDefinitionFactory.get_eventstream_create_request(eventstream_name,
                                                                workspace['id'],
                                                                eventhouse['id'],
                                                                kql_database)
        
        FabricRestApi.create_item(workspace['id'], create_eventstream_request)

        realtime_dashboard_create_request = ItemDefinitionFactory.get_kql_dashboard_create_request(
            realtime_dashboard_name,
            workspace['id'],
            kql_database,
            query_service_uri)
        
        FabricRestApi.create_item(workspace['id'], realtime_dashboard_create_request)

        create_queryset_create_request = ItemDefinitionFactory.get_kql_queryset_create_request(
            kql_queryset_name,
            kql_database,
            query_service_uri,
            'RealTimeQueryset.json'
        )

        FabricRestApi.create_item(workspace['id'], create_queryset_create_request)

        template_file_path = 'SemanticModels//bikes_rti_model.bim'
        bim_model_template = ItemDefinitionFactory.get_template_file(template_file_path)

        bim_model = bim_model_template.replace('{QUERY_SERVICE_URI}', query_service_uri)\
                                    .replace('{KQL_DATABASE_ID}', kql_database['id'])
        
        model_create_request = \
            ItemDefinitionFactory.get_semantic_model_create_request_from_definition(
                semantic_model_name,
                bim_model)

        model = FabricRestApi.create_item(workspace['id'], model_create_request)

        FabricRestApi.patch_oauth_connection_to_kqldb(workspace, model, query_service_uri)

        create_report_request = \
            ItemDefinitionFactory.get_report_create_request(model['id'],
                                                            report_name,
                                                            'rental_bike_sales.json')

        FabricRestApi.create_item(workspace['id'], create_report_request)

        AppLogger.log_job_complete(workspace['id'])

        return workspace

    @classmethod
    def deploy_medallion_warehouse_solution(
            cls,
            target_workspace,
            deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Medallion Warehouse Solution"""

        bronze_lakehouse_name = "sales_bronze"
        silver_lakehouse_name = "sales_silver"
        gold_warehouse_name = "sales"

        semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report',
            'Product Sales Top 10 Cities.Report'
        ]

        AppLogger.log_job(f"Deploying FabCon Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters()

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom FabCon Solution')

        data_prep_folder = FabricRestApi.create_folder(workspace['id'], 'data_prep')
        data_prep_folder_id = data_prep_folder['id']

        bronze_lakehouse = FabricRestApi.create_lakehouse(
            workspace['id'], 
            bronze_lakehouse_name,
            data_prep_folder_id)

        adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
        adls_path = f'/{adls_container_name}{adls_container_path}'

        connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            adls_server,
            adls_path,
            workspace)

        shortcut_name = "sales-data"
        shortcut_path = "Files"
        shortcut_location = adls_server
        shortcut_subpath = adls_path

        FabricRestApi.create_adls_gen2_shortcut(
            workspace['id'],
            bronze_lakehouse['id'],
            shortcut_name,
            shortcut_path,
            shortcut_location,
            shortcut_subpath,
            connection['id'])

        silver_lakehouse = FabricRestApi.create_lakehouse(
            workspace['id'],
            silver_lakehouse_name,
            data_prep_folder_id)

        FabricRestApi.create_onelake_shortcut(
            workspace['id'],
            silver_lakehouse['id'],
            bronze_lakehouse['id'],
            shortcut_name,
            shortcut_path)
        
        create_notebook_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
            'Build Silver.Notebook')
        
        notebook_redirects = {
            '{WORKSPACE_ID}': workspace['id'],
            '{LAKEHOUSE_ID}': silver_lakehouse['id'],
            '{LAKEHOUSE_NAME}': silver_lakehouse['displayName']
        }

        create_notebook_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_notebook_request,
                'notebook-content.py', 
                notebook_redirects)

        notebook = FabricRestApi.create_item(
            workspace['id'], 
            create_notebook_request,
            data_prep_folder_id)

        FabricRestApi.run_notebook(workspace['id'], notebook)

        silver_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], silver_lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], silver_sql_endpoint['database'])

        gold_warehouse = FabricRestApi.create_warehouse(
                workspace['id'],
                gold_warehouse_name
            )

        warehouse_connect_string = FabricRestApi.get_warehouse_connection_string(
            workspace['id'],
            gold_warehouse['id'])
        
        data_pipelines = [
            { 'name': 'Create Warehouse Tables', 'template':'CreateWarehouseTables.json'},
            { 'name': 'Create Warehouse Stored Procedures', 'template':'CreateWarehouseStoredProcs.json'},
            { 'name': 'Refresh Warehouse Tables', 'template':'RefreshWarehouseTables.json'}
        ]
        
        for data_pipeline in data_pipelines:
            template_file = f"DataPipelines//{data_pipeline['template']}"
            template_content = ItemDefinitionFactory.get_template_file(template_file)
            template_content = template_content.replace('{WORKSPACE_ID}', workspace['id']) \
                                                .replace('{LAKEHOUSE_ID}', silver_lakehouse['id']) \
                                                .replace('{WAREHOUSE_ID}', gold_warehouse['id']) \
                                                .replace('{WAREHOUSE_CONNECT_STRING}', warehouse_connect_string) \
                                                .replace('{CONNECTION_ID}', connection['id']) \
                                                .replace('{CONTAINER_NAME}', adls_container_name) \
                                                .replace('{CONTAINER_PATH}', adls_container_path)                                                         

            pipeline_create_request = ItemDefinitionFactory.get_data_pipeline_create_request(
                data_pipeline['name'],
                template_content)
            
            pipeline = FabricRestApi.create_item(
                workspace['id'], 
                pipeline_create_request,
                data_prep_folder_id)
            
            FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                semantic_model_folder)

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': warehouse_connect_string,
            '{SQL_ENDPOINT_DATABASE}': gold_warehouse['id']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_model_request,
                'definition/expressions.tmdl',
                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], gold_warehouse)

        for report_folder in report_folders:
            
            create_report_request = \
                ItemDefinitionFactory.get_create_report_request_from_folder(
                    report_folder,
                    model['id'])

            FabricRestApi.create_item(workspace['id'], create_report_request)
 
        return workspace

    @classmethod
    def deploy_notebook_solution_with_varlib(
        cls,
        target_workspace,
        deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Notebook Solution"""

        lakehouse_name = "sales"

        AppLogger.log_job(f"Deploying Custom Notebook Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters('web')

        workspace = FabricRestApi.create_workspace(target_workspace)
        
        FabricRestApi.update_workspace_description(workspace['id'], 'Custom Notebook Solution with Variable Library')

        web_datasource_path = deploy_job.parameters[deploy_job.web_datasource_path_parameter]
        
        variable_library = VariableLibrary()
        variable_library.add_variable("web_datasource_path", web_datasource_path)
        
        create_library_request = \
            ItemDefinitionFactory.get_variable_library_create_request(
                "environment_settings",
                variable_library
        )

        FabricRestApi.create_item(workspace['id'], create_library_request)

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        create_notebook_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Create Lakehouse Tables with VarLib.Notebook')

        notebook_redirects = {
            '{WORKSPACE_ID}': workspace['id'],
            '{LAKEHOUSE_ID}': lakehouse['id'],
            '{LAKEHOUSE_NAME}': lakehouse['displayName']
        }

        create_notebook_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_notebook_request,
                                                                'notebook-content.py', 
                                                                notebook_redirects)

        notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)

        FabricRestApi.run_notebook(workspace['id'], notebook)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Product Sales DirectLake Model.SemanticModel')

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
            '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_model_request,
                                                                'definition/expressions.tmdl',
                                                                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        create_report_request = \
            ItemDefinitionFactory.get_create_report_request_from_folder(
                'Product Sales Summary.Report',
                model['id'])

        FabricRestApi.create_item(workspace['id'], create_report_request)

        return workspace

    @classmethod
    def deploy_shortcut_solution_with_varlib(cls,
                                 target_workspace,
                                 deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Shortcut Solution with VarLib"""

        lakehouse_name = "sales"
        notebook_folders = [
            'Create 01 Silver Layer.Notebook',
            'Create 02 Gold Layer.Notebook'
        ]
        semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report'
        ]

        AppLogger.log_job(f"Deploying Custom Shortcut with VarLib Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters("adls")

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom Shortcut Solution')

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
        adls_path = f'/{adls_container_name}{adls_container_path}'

        connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            adls_server,
            adls_path,
            workspace)

        shortcut_name = "sales-data"
        shortcut_path = "Files"
        shortcut_location = adls_server
        shortcut_subpath = adls_path
     
        variable_library = VariableLibrary()
        variable_library.add_variable("adls_shortcut_location", adls_server)
        variable_library.add_variable("adls_shortcut_subpath",    adls_path)
        variable_library.add_variable("adls_connection_id",    connection['id'])

        create_library_request = \
            ItemDefinitionFactory.get_variable_library_create_request(
                "environment_settings",
                variable_library
        )

        FabricRestApi.create_item(workspace['id'], create_library_request)

        adls_shortcut_location_variable = "$(/**/environment_settings/adls_shortcut_location)"
        adls_shortcut_subpath_variable = "$(/**/environment_settings/adls_shortcut_subpath)"
        adls_connection_id_variable = "$(/**/environment_settings/adls_connection_id)"

        FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                                lakehouse['id'],
                                                shortcut_name,
                                                shortcut_path,
                                                adls_shortcut_location_variable,
                                                adls_shortcut_subpath_variable,
                                                adls_connection_id_variable)

        for notebook_folder in notebook_folders:
            create_notebook_request = \
                ItemDefinitionFactory.get_create_notebook_request_from_folder(
                    notebook_folder,
                    workspace['id'],
                    lakehouse)

            notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
            FabricRestApi.run_notebook(workspace['id'], notebook)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                semantic_model_folder)

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
            '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(create_model_request,
                                                                'definition/expressions.tmdl',
                                                                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        for report_folder in report_folders:
            create_report_request = \
                ItemDefinitionFactory.get_create_report_request_from_folder(
                    report_folder,
                    model['id'])

            FabricRestApi.create_item(workspace['id'], create_report_request)

        return workspace

    # @classmethod
    # def deploy_notebook_solution_with_variable_library(cls,
    #                              target_workspace,
    #                              deploy_job = StagingEnvironments.get_dev_environment()):
    #     """Deploy Notebook Solution"""

    #     lakehouse_name = "sales"

    #     AppLogger.log_job(f"Deploying Custom Notebook Solution to [{target_workspace}]")

    #     deploy_job.display_deployment_parameters("web")

    #     workspace = FabricRestApi.create_workspace(target_workspace)

    #     FabricRestApi.update_workspace_description(workspace['id'], 'Custom Notebook Solution')

    #     lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

    #     web_datasource_path = deploy_job.parameters[deploy_job.web_datasource_path_parameter]
 
    #     variable_library = VariableLibrary()
    #     variable_library.add_variable("workspace_id", workspace['id'])
    #     variable_library.add_variable("lakehouse_id", lakehouse['id'])
    #     variable_library.add_variable("lakehouse_display_name", lakehouse['displayName'])
    #     variable_library.add_variable("web_datasource_path", web_datasource_path)


    #     create_library_request = \
    #         ItemDefinitionFactory.get_variable_library_create_request(
    #             "environment_settings",
    #             variable_library
    #     )

    #     FabricRestApi.create_item(workspace['id'], create_library_request)

    #     create_notebook_request = \
    #         ItemDefinitionFactory.get_create_item_request_from_folder(
    #             'Create Lakehouse Tables.Notebook')

    #     notebook_redirects = {
    #         '{WORKSPACE_ID}': workspace['id'],
    #         '{LAKEHOUSE_ID}': lakehouse['id'],
    #         '{LAKEHOUSE_NAME}': lakehouse['displayName']
    #     }

    #     create_notebook_request = \
    #         ItemDefinitionFactory.update_part_in_create_request(create_notebook_request,
    #                                                             'notebook-content.py', 
    #                                                             notebook_redirects)

    #     notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)

    #     FabricRestApi.run_notebook(workspace['id'], notebook)

    #     sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

    #     FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

    #     create_model_request = \
    #         ItemDefinitionFactory.get_create_item_request_from_folder(
    #             'Product Sales DirectLake Model.SemanticModel')

    #     model_redirects = {
    #         '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
    #         '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
    #     }

    #     create_model_request = \
    #         ItemDefinitionFactory.update_part_in_create_request(create_model_request,
    #                                                             'definition/expressions.tmdl',
    #                                                             model_redirects)

    #     model = FabricRestApi.create_item(workspace['id'], create_model_request)

    #     FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    #     create_report_request = \
    #         ItemDefinitionFactory.get_create_report_request_from_folder(
    #             'Product Sales Summary.Report',
    #             model['id'])

    #     FabricRestApi.create_item(workspace['id'], create_report_request)

    #     AppLogger.log_job_complete(workspace['id'])

    #     return workspace

    @classmethod
    def deploy_data_pipeline_solution_with_varlib(cls,
                                         target_workspace, 
                                         deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Data Pipeline Solution with Variable Library"""

        lakehouse_name = "sales"
        notebook_folders = [
            'Build 01 Silver Layer.Notebook',
            'Build 02 Gold Layer.Notebook'
        ]
        data_pipeline_name = 'Create Lakehouse Tables'
        semantic_model_folder = 'Product Sales DirectLake Model.SemanticModel'
        report_folders = [
            'Product Sales Summary.Report',
            'Product Sales Time Intelligence.Report',
            'Product Sales Top 10 Cities.Report'
        ]

        AppLogger.log_job(f"Deploying Custom Variable Library Solution to [{target_workspace}]")

        workspace = FabricRestApi.create_workspace(target_workspace)

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        notebook_ids = []
        for notebook_folder in notebook_folders:
            create_notebook_request = \
                ItemDefinitionFactory.get_create_notebook_request_from_folder(
                    notebook_folder,
                    workspace['id'],
                    lakehouse)

            notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
            notebook_ids.append(notebook['id'])

        web_datasource_path = deploy_job.parameters[deploy_job.web_datasource_path_parameter]
        adls_server = deploy_job.parameters[deploy_job.adls_server_parameter]
        adls_container_name = deploy_job.parameters[deploy_job.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[deploy_job.adls_container_path_parameter]
        adls_path = adls_container_name + adls_container_path

        connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
            adls_server,
            adls_path,
            workspace,
            top_level_step=True)

        variable_library = VariableLibrary()
        variable_library.add_variable("adls_server", adls_server)
        variable_library.add_variable("adls_container_name",    adls_container_name)
        variable_library.add_variable("adls_container_path",    adls_container_path)
        variable_library.add_variable("adls_connection_id",    connection['id'])
        variable_library.add_variable("lakehouse_id",    lakehouse['id'])
        variable_library.add_variable("notebook_id_build_silver",    notebook_ids[0])
        variable_library.add_variable("notebook_id_build_gold",    notebook_ids[1])

        create_library_request = \
            ItemDefinitionFactory.get_variable_library_create_request(
                "SolutionConfig",
                variable_library
        )

        FabricRestApi.create_item(workspace['id'], create_library_request)

        pipeline_definition = ItemDefinitionFactory.get_template_file(
            'DataPipelines//CreateLakehouseTablesWithVarLib.json')

        create_pipeline_request = \
            ItemDefinitionFactory.get_data_pipeline_create_request(data_pipeline_name,
                                                                pipeline_definition)

        pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)
        FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

        FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

        create_model_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                semantic_model_folder)

        model_redirects = {
            '{SQL_ENDPOINT_SERVER}': sql_endpoint['server'],
            '{SQL_ENDPOINT_DATABASE}': sql_endpoint['database']
        }

        create_model_request = \
            ItemDefinitionFactory.update_part_in_create_request(
                create_model_request,
                'definition/expressions.tmdl',
                model_redirects)

        model = FabricRestApi.create_item(workspace['id'], create_model_request)

        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

        for report_folder in report_folders:
            create_report_request = \
                ItemDefinitionFactory.get_create_report_request_from_folder(
                    report_folder,
                    model['id'])

            FabricRestApi.create_item(workspace['id'], create_report_request)

        return workspace


    @classmethod
    def get_deployment_pipeline_by_name(cls, display_name):
        """Get Deployment Pipeline by Name"""
        for pipeline in FabricRestApi.list_deployment_pipelines():
            if pipeline['displayName'] == display_name:
                return pipeline
    
        return None

    @classmethod
    def delete_deployment_pipeline_by_name(cls, display_name):
        """Delete Deployment Pipeline by Name"""
        pipeline = FabricRestApi.get_deployment_pipeline_by_name(display_name)
        if pipeline is not None:
            AppLogger.log_step(f"Deleting existing deployment pipeline [{pipeline['displayName']}]")
            stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])
            for stage in stages:
                FabricRestApi.unassign_workpace_from_pipeline_stage(pipeline['id'], stage['id'])

            FabricRestApi.delete_deployment_pipeline(pipeline['id'])
            AppLogger.log_substep('Deployment pipeline deleted')

    @classmethod
    def delete_all_deployment_pipelines(cls):
        """Delete All Deployment Pipelines"""
        AppLogger.log_step("Deleting Pipelines")
        for pipeline in FabricRestApi.list_deployment_pipelines():
            AppLogger.log_substep(f"Deleting {pipeline['displayName']}")
            stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])
            for stage in stages:
                FabricRestApi.unassign_workpace_from_pipeline_stage(pipeline['id'], stage['id'])

            FabricRestApi.delete_deployment_pipeline(pipeline['id'])

    @classmethod
    def delete_all_workspaces(cls):
        """Delete All Workspaces"""
        AppLogger.log_step("Deleting workspaces and their associated connections")
        for workspace in FabricRestApi.list_workspaces():
            AppLogger.log_substep(f"Deleting workspace {workspace['displayName']} [{workspace['id']}]")
            FabricRestApi.delete_workspace(workspace['id'])

    @classmethod
    def delete_all_connections(cls):
        """Delete All Connections"""
        AppLogger.log_step("Deleting connections")
        for connection in FabricRestApi.list_connections():
            display_name = connection['displayName'] if connection['displayName'] else connection['id']
            AppLogger.log_substep(f"Deleting {display_name}")
            FabricRestApi.delete_connection(connection['id'])

    @classmethod
    def delete_all_github_repos(cls):
        """"Delete All GitHub Repos"""
        AppLogger.log_step("Deleting Demo GitHub Repos")
 
        repos = GitHubRestApi.get_github_repositories()
        for repo in repos:
            AppLogger.log_substep(f"Deleting {repo['name']}")
            GitHubRestApi.delete_github_repository(repo['name'])

    @classmethod 
    def delete_all_ado_projects(cls):
        """Delete All Azure DevOps Projects"""
        AppLogger.log_step("Deleting Demo Azure DevOps projects")
 
        projects = AdoProjectManager.get_projects()
        for project in projects:
            AdoProjectManager.delete_project(project['id'])

    @classmethod
    def cleanup_dev_environment(cls):
        """Clean Up Dev Environment"""
        AppLogger.log_job("Cleanup dev environment")
        cls.delete_all_deployment_pipelines()
        cls.delete_all_workspaces()
        cls.delete_all_connections()
        cls.delete_all_github_repos()
        cls.delete_all_ado_projects()
        AppLogger.log_job_ended("Cleanup of dev environment complete")

    @classmethod
    def setup_deployment_pipeline(cls, project_name, solution_name):
        """Setup Deployment Pipeline"""

        pipeline_name = project_name
        dev_workspace_name = f'{project_name}-dev'
        test_workspace_name = f'{project_name}-test'
        prod_workspace_name = project_name

        pipeline_stages = [ 'dev', 'test', 'prod' ]

        dev_workspace = DeploymentManager.deploy_solution_by_name(                
            solution_name, 
            dev_workspace_name
        )
        
        AppLogger.log_job(f"Setup deployment pipeline [{project_name}] based on [{solution_name}]")

        pipeline = FabricRestApi.create_deployment_pipeline(pipeline_name, pipeline_stages)

        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])

        for stage in stages:
            if stage['order'] == 0:
                FabricRestApi.assign_workpace_to_pipeline_stage(dev_workspace['id'],
                                                                pipeline['id'],
                                                                stage['id'])
            elif stage['order'] == 1:
                test_workspace = FabricRestApi.create_workspace(test_workspace_name)
                FabricRestApi.assign_workpace_to_pipeline_stage(test_workspace['id'],
                                                                pipeline['id'],
                                                                stage['id'])
            elif stage['order'] == 2:
                prod_workspace = FabricRestApi.create_workspace(prod_workspace_name)
                FabricRestApi.assign_workpace_to_pipeline_stage(prod_workspace['id'],
                                                                pipeline['id'],
                                                                stage['id'])

    @classmethod
    def deploy_from_dev_to_test(cls, pipeline_name):
        """Deploy Stage from Dev to Test"""
        pipeline = FabricRestApi.get_deployment_pipeline_by_name(pipeline_name)
        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])

        source_stage_id = stages[0]['id']
        target_stage_id = stages[1]['id']

        AppLogger.log_step("Deploy from [dev] to [test]")
        FabricRestApi.deploy_to_pipeline_stage(pipeline['id'], source_stage_id, target_stage_id)
        AppLogger.log_substep("Deploy operation complete")

    @classmethod
    def update_imported_semantic_model_source(cls, workspace,
                                                semantic_model_name, 
                                                web_datasource_path):
        """Update Imported Sementic Model Source"""
        model = FabricRestApi.get_item_by_name(workspace['id'], semantic_model_name, 'SemanticModel')
        old_web_datasource_path = FabricRestApi.get_web_url_from_semantic_model(workspace['id'], model['id']) + '/'

        if web_datasource_path == old_web_datasource_path:
            AppLogger.log_substep(f"Verified web datasource path: [{web_datasource_path}]")
        else:
            old_model_definition = FabricRestApi.get_item_definition(workspace['id'], model)
    
            search_replace_terms = {
                old_web_datasource_path: web_datasource_path
            }

            model_definition = {
                'definition': ItemDefinitionFactory.update_item_definition_part(
                    old_model_definition['definition'],
                    'definition/expressions.tmdl',
                    search_replace_terms)
            }

            FabricRestApi.update_item_definition(workspace['id'], model, model_definition)

    @classmethod
    def update_directlake_semantic_model_source(cls,
                                                workspace_name,
                                                semantic_model_name,
                                                lakehouse_name):
        """Update DirectLake Sementic Model Source"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], lakehouse_name, 'Lakehouse')
        model = FabricRestApi.get_item_by_name(workspace['id'], semantic_model_name, 'SemanticModel')
        
        old_sql_endpoint =    FabricRestApi.get_sql_endpoint_from_semantic_model(
            workspace['id'],
            model['id']
        )

        new_sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(
            workspace['id'],
            lakehouse)

        old_model_definition = FabricRestApi.get_item_definition(workspace['id'], model)

        search_replace_terms = {
            old_sql_endpoint['server']: new_sql_endpoint['server'],
            old_sql_endpoint['database']: new_sql_endpoint['database']
        }

        model_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part(
                old_model_definition['definition'],
                'definition/expressions.tmdl',
                search_replace_terms)
        }

        FabricRestApi.update_item_definition(workspace['id'], model, model_definition)

    @classmethod
    def update_datasource_path_in_notebook(cls,
                workspace_name,
                notebook_name,
                deployment_job):
        """Update datasource path in notebook"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        notebook = FabricRestApi.get_item_by_name(workspace['id'], notebook_name, 'Notebook')
        datasource_path = deployment_job.parameters[DeploymentJob.web_datasource_path_parameter]

        path1 = StagingEnvironments.get_dev_environment().parameters[DeploymentJob.web_datasource_path_parameter]
        path2 = StagingEnvironments.get_test_environment().parameters[DeploymentJob.web_datasource_path_parameter]
        path3 = StagingEnvironments.get_prod_environment().parameters[DeploymentJob.web_datasource_path_parameter]

        search_replace_terms = {
            path1: datasource_path,
            path2: datasource_path,
            path3: datasource_path,
        }
        
        old_notebook_definition = FabricRestApi.get_item_definition(workspace['id'], notebook)

        notebook_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part(
                old_notebook_definition['definition'],
                'notebook-content.py',
                search_replace_terms)
        }

        FabricRestApi.update_item_definition(workspace['id'], notebook, notebook_definition)        

    @classmethod
    def update_source_lakehouse_in_notebook(cls,
                workspace_name,
                notebook_name,
                lakehouse_name):
        """Update datasource path in notebook"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        notebook = FabricRestApi.get_item_by_name(workspace['id'], notebook_name, 'Notebook')
        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], lakehouse_name, "Lakehouse")

        workspace_id = workspace['id']
        lakehouse_id = lakehouse['id']

        search_replace_terms = {
            r'("default_lakehouse"\s*:\s*)".*"': rf'\1"{lakehouse_id}"',
            r'("default_lakehouse_name"\s*:\s*)".*"': rf'\1"{lakehouse_name}"',
            r'("default_lakehouse_workspace_id"\s*:\s*)".*"': rf'\1"{workspace_id}"',
            r'("known_lakehouses"\s*:\s*)\[[\s\S]*?\]': rf'\1[{{"id": "{lakehouse_id}"}}]',
        }

        notebook_definition = FabricRestApi.get_item_definition(workspace['id'], notebook)

        notebook_definition = {
            'definition': ItemDefinitionFactory.update_item_definition_part_with_regex(
                notebook_definition['definition'],
                'notebook-content.py',
                search_replace_terms)
        }

        FabricRestApi.update_item_definition(workspace['id'], notebook, notebook_definition)


    @classmethod
    def apply_post_deploy_fixes(cls,
                                workspace_name,
                                deployment_job,
                                run_etl_jobs = False):
        
        """Apply Post Deploy Fixes"""                    
        AppLogger.log_step(f"Applying post deploy fixes to [{workspace_name}]")
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        workspace_items = FabricRestApi.list_workspace_items(workspace['id'])

        adls_container_name = deployment_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deployment_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_server = deployment_job.parameters[DeploymentJob.adls_server_parameter]
        adls_path = f'/{adls_container_name}{adls_container_path}'

        lakehouses = list(filter(lambda item: item['type']=='Lakehouse', workspace_items))
        for lakehouse in lakehouses:
            shortcuts = FabricRestApi.list_shortcuts(workspace['id'], lakehouse['id'])
            for shortcut in shortcuts:
                connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                                adls_server,
                                adls_path,
                                workspace)

                shortcut_name = shortcut['name']
                shortcut_path = shortcut['path']
                shortcut_location = adls_server
                shortcut_subpath = adls_path

                FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                                        lakehouse['id'],
                                                        shortcut_name,
                                                        shortcut_path,
                                                        shortcut_location,
                                                        shortcut_subpath,
                                                        connection['id'])

        notebooks = list(filter(lambda item: item['type']=='Notebook', workspace_items))
        for notebook in notebooks:
            # Apply fixes for [Create Lakehouse Tables.Notebook]
            if notebook['displayName'] == 'Create Lakehouse Tables':
                cls.update_source_lakehouse_in_notebook(
                    workspace_name,
                    notebook['displayName'],
                    "sales")

                cls.update_datasource_path_in_notebook(
                    workspace_name,
                    notebook['displayName'],
                    deployment_job)
                
            if run_etl_jobs and 'Create' in notebook['displayName']:                
                FabricRestApi.run_notebook(workspace['id'], notebook)

        sql_endpoints =    list(filter(lambda item: item['type']=='SQLEndpoint', workspace_items))
        for sql_endpoint in sql_endpoints:
            FabricRestApi.refresh_sql_endpoint_metadata(
                workspace['id'],
                sql_endpoint['id'])

        models = list(filter(lambda item: item['type']=='SemanticModel', workspace_items))
        for model in models:

            # Apply fixes for [Product Sales Imported Model.SemanticModel]
            if model['displayName'] ==    'Product Sales Imported Model':
                # fix connection to imported models
                datasource_path =    \
                    deployment_job.parameters[deployment_job.web_datasource_path_parameter]

                DeploymentManager.update_imported_semantic_model_source(
                    workspace,
                    model['displayName'],
                    datasource_path)

                FabricRestApi.create_and_bind_semantic_model_connecton(
                    workspace,
                    model['id'])

            # Apply fixes for [Product Sales DirectLake Model.SemanticModel]
            if model['displayName'] ==    'Product Sales DirectLake Model':
                # fix connection to lakehouse SQL endpoint
                target_lakehouse_name = 'sales'
                DeploymentManager.update_directlake_semantic_model_source(
                    workspace_name, 
                    model['displayName'],
                    target_lakehouse_name)

                FabricRestApi.create_and_bind_semantic_model_connecton(
                    workspace,
                    model['id'])

    @classmethod
    def create_and_bind_model_connection(cls, workspace_name):
        """Create and Bind Model Connections"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        model_name = 'Product Sales DirectLake Model'
        model = FabricRestApi.get_item_by_name(workspace['id'], model_name, 'SemanticModel')
        FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'])

    @classmethod
    def deploy_from_test_to_prod(cls, pipeline_name):
        """Deploy Stage from Dev to Test"""
        pipeline = FabricRestApi.get_deployment_pipeline_by_name(pipeline_name)
        stages = FabricRestApi.list_deployment_pipeline_stages(pipeline['id'])

        source_stage_id = stages[1]['id']
        target_stage_id = stages[2]['id']

        AppLogger.log_step("Deploy from [test] to [prod]")
        FabricRestApi.deploy_to_pipeline_stage(pipeline['id'], source_stage_id, target_stage_id)
        AppLogger.log_substep("Deploy operation complete")

    @classmethod
    def update_variable_library(cls, workspace_name, library_name, deployment_job: DeploymentJob):
        """Update Variable Library"""

        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        variable_library_item = FabricRestApi.get_item_by_name(workspace['id'],
                                                        library_name,
                                                        'VariableLibrary')
        deployment_parameters = deployment_job.parameters

        variable_library_definition = FabricRestApi.get_item_definition(workspace['id'],
                                                                        variable_library_item)

        parts = variable_library_definition['definition']['parts']

        variables = None

        for part in parts:
            if part['path'] == 'variables.json':
                payload = part['payload']
                payload_bytes = base64.b64decode(payload)
                payload_content = payload_bytes.decode('utf-8')
                variables = json.loads(payload_content)['variables']
                break

        if variables is not None:
            variable_library    = VariableLibrary(variables)

            valueset = Valueset(deployment_job.name)

            for variable in variables:
                if variable['name'] in deployment_parameters:
                    variable_name = variable['name']
                    variable_override = deployment_parameters[variable_name]
                    valueset.add_variable_override(variable_name, variable_override)

            # set additional overrides with workspace-specific ids
            lakehouse_id_parameter = 'lakehouse_id'
            notebook_id_build_silver_parameter = 'notebook_id_build_silver'
            notebook_id_build_gold_parameter    = 'notebook_id_build_gold'
            adls_connection_id_parameter = 'adls_connection_id'

            lakehouse = FabricRestApi.get_item_by_name(workspace['id'], 'sales', 'Lakehouse')
            valueset.add_variable_override(lakehouse_id_parameter, lakehouse['id'])

            notebook_build_silver = FabricRestApi.get_item_by_name(
                workspace['id'],
                'Build 01 Silver Layer', 
                'Notebook')

            valueset.add_variable_override(
                notebook_id_build_silver_parameter,
                notebook_build_silver['id'])

            notebook_build_gold = FabricRestApi.get_item_by_name(
                workspace['id'],
                'Build 02 Gold Layer', 
                'Notebook')

            valueset.add_variable_override(
                notebook_id_build_gold_parameter,
                notebook_build_gold['id'])

            adls_server = deployment_job.parameters[DeploymentJob.adls_server_parameter]
            adls_container_name = \
                deployment_job.parameters[DeploymentJob.adls_container_name_parameter]
            adls_container_path = \
                deployment_job.parameters[DeploymentJob.adls_container_path_parameter]
            adls_server_path = adls_container_name + adls_container_path

            connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
                adls_server,
                adls_server_path,
                workspace)

            valueset.add_variable_override(adls_connection_id_parameter, connection['id'])

            variable_library.add_valueset(valueset)

            update_request = \
                ItemDefinitionFactory.get_update_variable_library_request(variable_library)

            FabricRestApi.update_item_definition(
                workspace['id'],
                variable_library_item,
                update_request)

            FabricRestApi.set_active_valueset_for_variable_library(
                workspace['id'],
                variable_library_item,
                deployment_job.name)

            # run data pipeline
            data_pipeline_name = 'Create Lakehouse Tables'
            data_pipeline = FabricRestApi.get_item_by_name(
                workspace['id'],
                data_pipeline_name,
                'DataPipeline')
            FabricRestApi.run_data_pipeline(workspace['id'], data_pipeline)

        else:
            AppLogger.log_error("Error running data pipeline")

    @classmethod
    def run_data_pipeline(cls, workspace_name, data_pipeline_name):
        """Run data pipeline"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        data_pipeline_name = 'Create Lakehouse Tables'
        data_pipeline = FabricRestApi.get_item_by_name(
            workspace['id'],
            data_pipeline_name,
            'DataPipeline')

        FabricRestApi.run_data_pipeline(workspace['id'], data_pipeline)

    @classmethod
    def get_sql_endpoint_info_by_name(cls, workspace_name, lakehouse_name):
        """Get SQL Endpoint"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], 
                                                     lakehouse_name, 'Lakehouse')
        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)
    
    @classmethod
    def conn_filter_for_github(cls, connection):
        """GitHub connection filter"""
        return connection['connectionDetails']['type'] ==    'GitHubSourceControl'

    @classmethod
    def delete_all_github_connections(cls):
        """Delete All GitHub connection"""
        github_connections = list(filter(cls.conn_filter_for_github, FabricRestApi.list_connections()))
        for connection in github_connections:
            AppLogger.log_step(f"Deleting connection {connection['displayName']}")
            FabricRestApi.delete_connection(connection['id'])

    @classmethod
    def sync_workspace_to_github_repo(cls, workspace, repo_name = None):
        """Setup Workspace with GIT Connection"""
        
        if repo_name is None:
            repo_name = workspace['displayName'].replace(" ", "-")
        
        AppLogger.log_job(f"Configuring GIT integration in GitHub for workspace [{workspace['displayName']}]")

        GitHubRestApi.create_repository(repo_name, add_secrets=True)
        GitHubRestApi.create_branch(repo_name, 'test')
        GitHubRestApi.create_branch(repo_name, 'dev')
        GitHubRestApi.set_default_branch(repo_name, 'dev')
        FabricRestApi.connect_workspace_to_github_repo(workspace, repo_name, 'dev')

        AppLogger.log_job_complete(workspace['id'])

    # @classmethod
    # def connect_workspace_to_ado_repo_branch(cls, workspace, project_name = None):
    #     """Setup Workspace with GIT Connection"""
        
    #     if project_name is None:
    #         project_name = workspace['displayName'].replace(" ", "-")

    #     AppLogger.log_job(f"Setup workspace [{workspace['displayName']} with GIT integration]")

    #     AdoProjectManager.create_project_with_pipelines(project_name)

    #     FabricRestApi.connect_workspace_to_ado_repo(workspace, project_name, 'dev')

    #     AppLogger.log_job_complete(workspace['id'])

    @classmethod
    def sync_workspace_to_ado_repo(cls, workspace, project_name = None):
        """Setup Git Workspace Connecton to ADO repo"""
        
        if project_name is None:
            project_name = workspace['displayName'].replace(" ", "-")

        AppLogger.log_job(f"Configuring GIT integration in Azure DevOps for workspace [{workspace['displayName']}]")

        AdoProjectManager.create_project(project_name)
        AdoProjectManager.create_branch(project_name, 'test', 'main')
        AdoProjectManager.create_branch(project_name, 'dev', 'test')
        AdoProjectManager.set_default_branch(project_name, 'dev')
        FabricRestApi.connect_workspace_to_ado_repo(workspace, project_name, 'dev')

        AppLogger.log_job_complete(workspace['id'])


    @classmethod
    def generate_parameter_yml_file(
        cls,
        dev_workspace_name,
        test_workspace_name,
        prod_workspace_name):
        """Generate parameter.yml file"""

        dev_workspace = FabricRestApi.get_workspace_by_name(dev_workspace_name)
        dev_workspace_items = FabricRestApi.list_workspace_items(dev_workspace['id'])
        
        test_workspace = FabricRestApi.get_workspace_by_name(test_workspace_name)
        test_workspace_items    = FabricRestApi.list_workspace_items(test_workspace['id'])

        test_items = {}
        for test_item in test_workspace_items:
            item_name = test_item['displayName'] + "." + test_item['type']
            test_items[item_name] = test_item['id']
    
        prod_workspace = FabricRestApi.get_workspace_by_name(prod_workspace_name)
        prod_workspace_items    = FabricRestApi.list_workspace_items(prod_workspace['id'])

        prod_items = {}
        for prod_item in prod_workspace_items:
            item_name = prod_item['displayName'] + "." + prod_item['type']
            prod_items[item_name] = prod_item['id']
    
        tab = (' ' * 4)
        file_content = 'find_replace:\n\n'

        file_content += tab + '# [Workspace Id]\n'
        file_content += tab + '- find_value: "' + dev_workspace['id'] + f'" # [{dev_workspace["displayName"]}]\n'
        file_content += tab + '    replace_value:\n'
        file_content += tab + tab + f'TEST: "{test_workspace["id"]}" # [{test_workspace["displayName"]}]\n'
        file_content += tab + tab + f'PROD: "{prod_workspace["id"]}" # [{prod_workspace["displayName"]}]\n\n'

        for workspace_item in dev_workspace_items:
            item_name = workspace_item['displayName'] + "." + workspace_item['type']
            file_content += tab + f'# [{item_name}]\n'
            file_content += tab + '- find_value: "' + workspace_item['id'] + f'" # [{dev_workspace["displayName"]}]\n'
            file_content += tab + '    replace_value:\n'
            if item_name in test_items:
                file_content += tab + tab + f'TEST: "{test_items[item_name]}" # [{test_workspace["displayName"]}]\n'
            if item_name in prod_items:
                file_content += tab + tab + f'PROD: "{prod_items[item_name]}" # [{prod_workspace["displayName"]}]\n\n'

            if workspace_item['type'] == 'SemanticModel':

                dev_datasources = FabricRestApi.get_datasources_for_semantic_model(
                    dev_workspace['id'],
                    workspace_item['id']
                )

                test_datasources = FabricRestApi.get_datasources_for_semantic_model(
                    test_workspace['id'],
                    test_items[item_name]
                )

                prod_datasources = FabricRestApi.get_datasources_for_semantic_model(
                    prod_workspace['id'],
                    prod_items[item_name]
                )

                for index, dev_datasource in enumerate(dev_datasources):
                    if dev_datasource['datasourceType'] == "Web":
                        dev_url = dev_datasource['connectionDetails']['url']
                        test_url = test_datasources[index]['connectionDetails']['url']
                        prod_url = prod_datasources[index]['connectionDetails']['url']
                        file_content += tab + '# [Web Datasource Url]\n'
                        file_content += tab + '- find_value: "' + dev_url + f'" # [{dev_workspace["displayName"]}]\n'
                        file_content += tab + '    replace_value:\n'
                        file_content += tab + tab + f'TEST: "{test_url}" # [{test_workspace["displayName"]}]\n'
                        file_content += tab + tab + f'PROD: "{prod_url}" # [{prod_workspace["displayName"]}]\n\n'

                    if dev_datasource['datasourceType'] == "Sql":
                        dev_sql_server = dev_datasource['connectionDetails']['server']
                        dev_sql_database = dev_datasource['connectionDetails']['database']
                        test_sql_server = test_datasources[index]['connectionDetails']['server']
                        test_sql_database = test_datasources[index]['connectionDetails']['database']
                        prod_sql_server = prod_datasources[index]['connectionDetails']['server']
                        prod_sql_database = prod_datasources[index]['connectionDetails']['database']

                        file_content += tab + '# [SQL Datasource Server]\n'
                        file_content += tab + '- find_value: "' + dev_sql_server + f'" # [{dev_workspace["displayName"]}]\n'
                        file_content += tab + '    replace_value:\n'
                        file_content += tab + tab + f'TEST: "{test_sql_server}" # [{test_workspace["displayName"]}]\n'
                        file_content += tab + tab + f'PROD: "{prod_sql_server}" # [{prod_workspace["displayName"]}]\n\n'

                        file_content += tab + '# [SQL Datasource Database]\n'
                        file_content += tab + '- find_value: "' + dev_sql_database + f'" # [{dev_workspace["displayName"]}]\n'
                        file_content += tab + '    replace_value:\n'
                        file_content += tab + tab + f'TEST: "{test_sql_database}" # [{test_workspace["displayName"]}]\n'
                        file_content += tab + tab + f'PROD: "{prod_sql_database}" # [{prod_workspace["displayName"]}]\n\n'

        return file_content

    @classmethod
    def generate_parameter_yml_file_backup(
        cls,
        dev_workspace_name,
        test_workspace_name,
        prod_workspace_name):
        """Generate parameter.yml file"""

        dev_workspace = FabricRestApi.get_workspace_by_name(dev_workspace_name)
        dev_workspace_items = FabricRestApi.list_workspace_items(dev_workspace['id'])
        
        test_workspace = FabricRestApi.get_workspace_by_name(test_workspace_name)
        test_workspace_items    = FabricRestApi.list_workspace_items(test_workspace['id'])

        test_items = {}
        for test_item in test_workspace_items:
            item_name = test_item['displayName'] + "." + test_item['type']
            test_items[item_name] = test_item['id']
    
        prod_workspace = FabricRestApi.get_workspace_by_name(prod_workspace_name)
        prod_workspace_items    = FabricRestApi.list_workspace_items(prod_workspace['id'])

        prod_items = {}
        for prod_item in prod_workspace_items:
            item_name = prod_item['displayName'] + "." + prod_item['type']
            prod_items[item_name] = prod_item['id']
    
        tab = (' ' * 4)
        file_content = 'find_replace:\n\n'

        file_content += tab + '# [Workspace Id]\n\n'
        file_content += tab + '- find_value: "' + dev_workspace['id'] + f'" # [{dev_workspace["displayName"]}]\n'
        file_content += tab + '    replace_value:\n'
        file_content += tab + tab + f'TEST: "{test_workspace["id"]}" # [{test_workspace["displayName"]}]\n'
        file_content += tab + tab + f'PROD: "{prod_workspace["id"]}" # [{prod_workspace["displayName"]}]\n\n'

        for workspace_item in dev_workspace_items:
            item_name = workspace_item['displayName'] + "." + workspace_item['type']
            file_content += tab + f'# [{item_name}]\n'
            file_content += tab + '- find_value: "' + workspace_item['id'] + f'" # [{dev_workspace["displayName"]}]\n'
            file_content += tab + '    replace_value:\n'
            if item_name in test_items:
                file_content += tab + tab + f'TEST: "{test_items[item_name]}" # [{test_workspace["displayName"]}]\n\n'

            if workspace_item['type'] == 'SemanticModel':
                dev_datasource_path = FabricRestApi.get_web_url_from_semantic_model(
                    dev_workspace['id'],
                    workspace_item['id']
                )
                test_datasource_path = FabricRestApi.get_web_url_from_semantic_model(
                    test_workspace['id'],
                    test_items[item_name]
                )
                file_content += tab + '- find_value: "' + dev_datasource_path + f'" # [{dev_workspace["displayName"]}]\n'
                file_content += tab + '    replace_value:\n'
                file_content += tab + tab + f'TEST: "{test_datasource_path}" # [{test_workspace["displayName"]}]\n\n'

        for workspace_item in test_workspace_items:
            item_name = workspace_item['displayName'] + "." + workspace_item['type']
            file_content += tab + f'# [{item_name}]\n'
            file_content += tab + '- find_value: "' + workspace_item['id'] + f'" # [{test_workspace["displayName"]}]\n'
            file_content += tab + '    replace_value:\n'
            if item_name in prod_items:
                file_content += tab + tab + f'PROD: "{test_items[item_name]}" # [{prod_workspace["displayName"]}]\n\n'

            if workspace_item['type'] == 'SemanticModel':
                test_datasource_path = FabricRestApi.get_web_url_from_semantic_model(
                    test_workspace['id'],
                    workspace_item['id']
                )
                prod_datasource_path = FabricRestApi.get_web_url_from_semantic_model(
                    prod_workspace['id'],
                    prod_items[item_name]
                )
                file_content += tab + '- find_value: "' + test_datasource_path + f'" # [{test_workspace["displayName"]}]\n'
                file_content += tab + '    replace_value:\n'
                file_content += tab + tab + f'PROD: "{prod_datasource_path}" # [{prod_workspace["displayName"]}]\n\n'


        return file_content

    @classmethod
    def generate_workspace_config_file(
        cls,
        dev_workspace_name,
        test_workspace_name,
        prod_workspace_name):
        """Generate workspace config"""

        dev_workspace = FabricRestApi.get_workspace_by_name(dev_workspace_name)
        test_workspace = FabricRestApi.get_workspace_by_name(test_workspace_name)
        prod_workspace = FabricRestApi.get_workspace_by_name(prod_workspace_name)

                
        config    = {
            'dev': {
                'workspace_id': dev_workspace['id'],
                'environment': 'DEV'
            },
            'test': {
                'workspace_id': test_workspace['id'],
                'environment': 'TEST'
            },
            'main': {
                'workspace_id': prod_workspace['id'],
                'environment': 'PROD'

            }
        }

        return json.dumps(config, indent=4)