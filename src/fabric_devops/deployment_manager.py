"""Deployment Manager"""

import base64
import json

from .app_logger import AppLogger
#from .app_settings import AppSettings
from .deployment_job import DeploymentJob
from .fabric_rest_api import FabricRestApi
from .item_definition_factory import ItemDefinitionFactory
from .variable_library import VariableLibrary, Valueset
from .staging_environments import StagingEnvironments
from .github_rest_api import GitHubRestApi

class DeploymentManager:
    """Deployment Manager"""

    @classmethod
    def deploy_powerbi_solution(cls, 
                                target_workspace, 
                                deploy_job = StagingEnvironments.get_dev_environment()):
        """Deploy Power BI Solution with Parameters"""

        AppLogger.log_job(f"Deploying Customer Power BI Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters()

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

        AppLogger.log_step("Power BI Solution deployment complete")

        return workspace

    @classmethod
    def deploy_notebook_solution(cls, target_workspace, deploy_job):
        """Deploy Notebook Solution"""

        lakehouse_name = "sales"

        AppLogger.log_job(f"Deploying Custom Notebook Solution to [{target_workspace}]")

        deploy_job.display_deployment_parameters()

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

        AppLogger.log_step("Notebook Solution deployment complete")

        return workspace

    @classmethod
    def deploy_shortcut_solution(cls, target_workspace, deploy_job):
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

        deploy_job.display_deployment_parameters()

        workspace = FabricRestApi.create_workspace(target_workspace)

        FabricRestApi.update_workspace_description(workspace['id'], 'Custom Shortcut Solution')

        lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

        adls_container_name = deploy_job.parameters[DeploymentJob.adls_container_name_parameter]
        adls_container_path = deploy_job.parameters[DeploymentJob.adls_container_path_parameter]
        adls_server = deploy_job.parameters[DeploymentJob.adls_server_parameter]
        adls_path = f'/{adls_container_name}{adls_container_path}'

        connection = FabricRestApi.create_azure_storage_connection_with_account_key(
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

        AppLogger.log_step("Shortcut Solution deployment complete")

        return workspace

    @classmethod
    def deploy_data_pipeline_solution(cls, target_workspace, deploy_job):
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

        deploy_job.display_deployment_parameters()

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


        connection = FabricRestApi.create_azure_storage_connection_with_account_key(
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
 
        AppLogger.log_step("Data Pipeline Solution deployment complete")

        return workspace

    @classmethod
    def deploy_variable_library_solution(cls, target_workspace, deploy_job):
        """Deploy Variable Library Solution"""

        # currently, this cannot run as SPN, it only works when running as user
        #AppSettings.RUN_AS_SERVICE_PRINCIPAL = False

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

        connection = FabricRestApi.create_azure_storage_connection_with_account_key(
            adls_server,
            adls_path,
            workspace,
            top_level_step=True)

        variable_library = VariableLibrary()
        variable_library.add_variable("web_datasource_path", web_datasource_path)
        variable_library.add_variable("adls_server", adls_server)
        variable_library.add_variable("adls_container_name",  adls_container_name)
        variable_library.add_variable("adls_container_path",  adls_container_path)
        variable_library.add_variable("adls_connection_id",  connection['id'])
        variable_library.add_variable("lakehouse_id",  lakehouse['id'])
        variable_library.add_variable("notebook_id_build_silver",  notebook_ids[0])
        variable_library.add_variable("notebook_id_build_gold",  notebook_ids[1])

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

        AppLogger.log_job_ended("Solution deployment complete")

        return workspace

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
        repos = GitHubRestApi.get_github_repositories()
        for repo in repos:
            GitHubRestApi.delete_github_repository(repo['name'])

    @classmethod
    def cleanup_dev_environment(cls):
        """Clean Up Dev Environment"""
        cls.delete_all_deployment_pipelines()
        cls.delete_all_workspaces()
        cls.delete_all_connections()
        cls.delete_all_github_repos()

    @classmethod
    def setup_deployment_pipeline(cls, pipeline_name):
        """Setup Deployment Pipeline"""

        dev_workspace_name = f'{pipeline_name}-dev'
        test_workspace_name = f'{pipeline_name}-test'
        prod_workspace_name = pipeline_name

        pipeline_stages = [ 'dev', 'test', 'prod' ]

        dev_workspace = FabricRestApi.get_workspace_by_name(dev_workspace_name)

        if dev_workspace is None:
            dev_env = StagingEnvironments.get_dev_environment()
            dev_workspace = DeploymentManager.deploy_variable_library_solution(
                dev_workspace_name,
                dev_env)

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
            variable_library  = VariableLibrary(variables)

            valueset = Valueset(deployment_job.name)

            for variable in variables:
                if variable['name'] in deployment_parameters:
                    variable_name = variable['name']
                    variable_override = deployment_parameters[variable_name]
                    valueset.add_variable_override(variable_name, variable_override)

            # set additional overrides with workspace-specific ids
            lakehouse_id_parameter = 'lakehouse_id'
            notebook_id_build_silver_parameter = 'notebook_id_build_silver'
            notebook_id_build_gold_parameter  = 'notebook_id_build_gold'
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

            connection = FabricRestApi.create_azure_storage_connection_with_account_key(
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
    def get_sql_endpoint_info(cls, workspace_name, lakehouse_name):
        """Get SQL Endpoint"""
        workspace = FabricRestApi.get_workspace_by_name(workspace_name)
        lakehouse = FabricRestApi.get_item_by_name(workspace['id'], 
                                                   lakehouse_name, 'Lakehouse')
        sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

  
    @classmethod
    def conn_filter(cls, connection): 
        """GitHub connection filter"""
        return connection['connectionDetails']['type'] ==  'GitHubSourceControl'

    @classmethod
    def delete_all_github_connections(cls):
        """Delete All GitHub connection"""
        github_connections = list(filter(cls.conn_filter, FabricRestApi.list_connections()))
        for connection in github_connections:
            AppLogger.log_step(f"Deleting connection {connection['displayName']}")
            FabricRestApi.delete_connection(connection['id'])

    @classmethod
    def setup_workspace_with_github_repo(cls, target_workspace):
        """Setup Workspace with GIT Connection"""

        workspace = FabricRestApi.get_workspace_by_name(target_workspace)

        if workspace is None:
            workspace = DeploymentManager.deploy_powerbi_solution(target_workspace)
        
        repo_name = target_workspace
        GitHubRestApi.create_repository(repo_name)
        GitHubRestApi.copy_files_from_folder_to_repo(repo_name, 'Hello')
        GitHubRestApi.create_branch(repo_name, 'test')
        GitHubRestApi.create_branch(repo_name, 'dev')
        GitHubRestApi.set_default_branch(repo_name, 'dev')

        FabricRestApi.connect_workspace_to_github_repo(workspace, target_workspace, 'dev')