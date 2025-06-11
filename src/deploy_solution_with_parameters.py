"""Deploy Solution with Parameters"""

import os

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, \
                          SampleCustomerData, DeploymentJob, VariableLibrary

def deploy_powerbi_solution(deploy_job):
    """Deploy Power BI Solution with Parameters"""

    workspace_name = deploy_job.target_workspace_name

    AppLogger.log_job(f"Deploying Customer Power BI Solution to [{workspace_name}]")

    deploy_job.display_deployment_parameters()

    workspace = FabricRestApi.create_workspace(workspace_name)

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

    FabricRestApi.bind_semantic_model_to_connection(workspace['id'], model['id'], connection['id'])

    FabricRestApi.refresh_semantic_model(workspace['id'], model['id'])

    create_report_request = \
        ItemDefinitionFactory.get_create_report_request_from_folder(
            'Product Sales Summary.Report',
            model['id'])

    FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_notebook_solution(deploy_job):
    """Deploy Notebook Solution"""

    workspace_name = deploy_job.target_workspace_name
    lakehouse_name = "sales"

    AppLogger.log_job(f"Deploying Custom Notebook Solution to [{workspace_name}]")

    deploy_job.display_deployment_parameters()

    workspace = FabricRestApi.create_workspace(workspace_name)

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

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_shortcut_solution(deploy_job):
    """Deploy Shortcut Solution"""

    workspace_name = deploy_job.target_workspace_name
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

    AppLogger.log_job(f"Deploying Custom Shortcut Solution to [{workspace_name}]")

    deploy_job.display_deployment_parameters()

    workspace = FabricRestApi.create_workspace(workspace_name)

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

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_data_pipeline_solution(deploy_job):
    """Deploy Data Pipeline Solution"""

    workspace_name = deploy_job.target_workspace_name
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

    AppLogger.log_job(f"Deploying Custom Data Pipeline Solution to [{workspace_name}]")

    deploy_job.display_deployment_parameters()

    workspace = FabricRestApi.create_workspace(workspace_name)

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

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_variable_library_solution(deploy_job):
    """Deploy Variable Library Solution"""

    workspace_name = deploy_job.target_workspace_name
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

    AppLogger.log_job(f"Deploying Custom Variable Library Solution to [{workspace_name}]")

    workspace = FabricRestApi.create_workspace(workspace_name)

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
        workspace)

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

    FabricRestApi.refresh_sql_endpoint_metadata(workspace['id'], sql_endpoint['database'])

    create_model_request = \
        ItemDefinitionFactory.get_directlake_model_create_request(semantic_model_folder,
                                                                  'sales_model_DirectLake.bim',
                                                                  sql_endpoint['server'],
                                                                  sql_endpoint['database'])

    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    for report_folder in report_folders:
        create_report_request = \
            ItemDefinitionFactory.get_create_report_request_from_folder(
                report_folder,
                model['id'])

        FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_warehouse_solution(deploy_job):
    """Deploy Warehouse Solution"""

    workspace_name = deploy_job.target_workspace_name
    
    lakehouse_name = "staging"
    warehouse_name = "sales"

    data_pipelines = [
        { 'name': 'Load Tables in Staging Lakehouse', 'template':'LoadTablesInStagingLakehouseWithParams.json'},
        { 'name': 'Create Warehouse Tables', 'template':'CreateWarehouseTables.json'},
        { 'name': 'Create Warehouse Stored Procedures', 'template':'CreateWarehouseStoredProcedures.json'},
        { 'name': 'Refresh Warehouse Tables', 'template':'RefreshWarehouseTables.json'}
    ]

    semantic_model_name = 'Product Sales DirectLake Model'
    reports = [
        { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'},
        { 'name': 'Product Sales Time Intelligence', 'template':'product_sales_time_intelligence.json'},
        { 'name': 'Product Sales Top 10 Cities', 'template':'product_sales_top_ten_cities_report.json'},
    ]

    AppLogger.log_job("Deploying Warehouse solution")

    workspace = FabricRestApi.create_workspace(workspace_name)

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

    createModelRequest = \
        ItemDefinitionFactory.get_directlake_model_create_request(semantic_model_name,
                                                                  'sales_model_DirectLake.bim',
                                                                  warehouse_connect_string,
                                                                  warehouse['id'])

    model = FabricRestApi.create_item(workspace['id'], createModelRequest)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    for report_data in reports:
        create_report_request = ItemDefinitionFactory.get_report_create_request(model['id'],
                                                                                report_data['name'],
                                                                                report_data['template'])
        report = FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

    return workspace

CUSTOMER_JOBS = []
match os.getenv("CUSTOMER_NAME"):
    case 'Adventure Works':
        CUSTOMER_JOBS = [SampleCustomerData.get_adventureworks()]
    case 'Contoso':
        CUSTOMER_JOBS = [SampleCustomerData.get_contoso()]
    case 'Fabrikam':
        CUSTOMER_JOBS = [SampleCustomerData.get_fabrikam()]
    case 'Northwind':
        CUSTOMER_JOBS = [SampleCustomerData.get_northwind()]
    case 'Seamark Farms':
        CUSTOMER_JOBS = [SampleCustomerData.get_seamarkfarms()]
    case 'Wingtip Toys':
        CUSTOMER_JOBS = [SampleCustomerData.get_wingtip()]
    case 'Deploy To All Customers':
        CUSTOMER_JOBS = SampleCustomerData.get_all_customers()

for CUSTOMER_JOB in CUSTOMER_JOBS:
    match os.getenv("SOLUTION_NAME"):
        case 'Custom Power BI Solution':
            deploy_powerbi_solution(CUSTOMER_JOB)
        case 'Custom Notebook Solution':
            deploy_notebook_solution(CUSTOMER_JOB)
        case 'Custom Shortcut Solution':
            deploy_shortcut_solution(CUSTOMER_JOB)
        case 'Custom Data Pipeline Solution':
            deploy_data_pipeline_solution(CUSTOMER_JOB)
        case 'Custom Variable Library Solution':
            deploy_variable_library_solution(CUSTOMER_JOB)
        case 'Custom Warehouse Solution':
            deploy_warehouse_solution(CUSTOMER_JOB)

