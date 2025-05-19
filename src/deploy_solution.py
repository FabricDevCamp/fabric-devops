"""Deploy Power BI Solution"""

import os

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, AppSettings, \
                          VariableLibrary

def deploy_powerbi_solution():
    """Deploy Power BI Solution"""

    AppLogger.log_job("Deploying Power BI Solution")

    workspace_name = "Custom Power BI Solution"
    semantic_model_name = 'Product Sales Imported Model'
    report_name = 'Product Sales Summary'

    workspace = FabricRestApi.create_workspace(workspace_name)

    create_model_request = \
        ItemDefinitionFactory.get_semantic_model_create_request(semantic_model_name,
                                                                'sales_model_import.bim')
    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    web_url = FabricRestApi.get_web_url_from_semantic_model(workspace['id'], model['id'])

    AppLogger.log_substep(f'Creating anonymous Web connection to {web_url} ')

    connection = FabricRestApi.create_anonymous_web_connection(web_url, workspace)

    FabricRestApi.bind_semantic_model_to_connection(workspace['id'], model['id'], connection['id'])

    FabricRestApi.refresh_semantic_model(workspace['id'], model['id'])

    create_report_request = \
        ItemDefinitionFactory.get_report_create_request(model['id'],
                                                        report_name,
                                                        'product_sales_summary.json')

    FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_notebook_solution():
    """Deploy Notebook Solution"""

    AppLogger.log_job("Deploying Lakehouse solution with Notebook")

    workspace_name = "Custom Notebook Solution"
    lakehouse_name = "sales"
    notebook_name = "Create Lakehouse Tables"
    semantic_model_name = 'Product Sales DirectLake Model'
    report_name = 'Product Sales Summary'

    workspace = FabricRestApi.create_workspace(workspace_name)
    lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

    create_notebook_request = \
        ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                        lakehouse,     \
                                                        notebook_name, \
                                                        'CreateLakehouseTables.py')
    notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
    FabricRestApi.run_notebook(workspace['id'], notebook)

    sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

    create_model_request = \
        ItemDefinitionFactory.get_directlake_model_create_request(semantic_model_name,
                                                                'sales_model_DirectLake.bim',
                                                                sql_endpoint['server'],
                                                                sql_endpoint['database'])

    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    create_report_request = \
        ItemDefinitionFactory.get_report_create_request(model['id'],
                                                        report_name,
                                                        'product_sales_summary.json')

    FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_shortcut_solution():
    """Deploy Shortcut Solution"""

    AppLogger.log_job("Deploying shortcut solution")

    workspace_name = "Custom Shortcut Solution"
    lakehouse_name = "sales"
    notebooks = [
        { 'name': 'Create 01 Silver Layer', 'template':'BuildSilverLayer.py'},
        { 'name': 'Create 02 Gold Layer', 'template':'BuildGoldLayer.py'},
    ]
    semantic_model_name = 'Product Sales DirectLake Model'
    reports = [
        {
            'name': 'Product Sales Summary', 
            'template':'product_sales_summary.json'
        },
        {
            'name': 'Product Sales Time Intelligence',
            'template':'product_sales_time_intelligence.json'},
    ]

    workspace = FabricRestApi.create_workspace(workspace_name)

    lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

    connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
        AppSettings.AZURE_STORAGE_SERVER,
        AppSettings.AZURE_STORAGE_PATH,
        workspace)

    shortcut_name = "sales-data"
    shortcut_path = "Files"
    shortcut_location = AppSettings.AZURE_STORAGE_SERVER
    shortcut_subpath = AppSettings.AZURE_STORAGE_PATH

    FabricRestApi.create_adls_gen2_shortcut(workspace['id'],
                                                    lakehouse['id'],
                                                    shortcut_name,
                                                    shortcut_path,
                                                    shortcut_location,
                                                    shortcut_subpath,
                                                    connection['id'])

    for notebook_data in notebooks:
        create_notebook_request = \
        ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                        lakehouse,     \
                                                        notebook_data['name'], \
                                                        notebook_data['template'])
        notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
        FabricRestApi.run_notebook(workspace['id'], notebook)

    sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

    create_model_request = \
        ItemDefinitionFactory.get_directlake_model_create_request(semantic_model_name,
                                                                'sales_model_DirectLake.bim',
                                                                sql_endpoint['server'],
                                                                sql_endpoint['database'])

    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    for report_data in reports:
        create_report_request = \
            ItemDefinitionFactory.get_report_create_request(model['id'],
                                                            report_data['name'],
                                                            report_data['template'])

        FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_data_pipeline_solution():
    """Deploy Data Pipeline Solution"""

    AppLogger.log_job("Deploying data pipeline solution")

    workspace_name = "Custom Data Pipeline Solution"
    lakehouse_name = "sales"
    notebooks = [
        { 'name': 'Create 01 Silver Layer', 'template':'BuildSilverLayer.py'},
        { 'name': 'Create 02 Gold Layer', 'template':'BuildGoldLayer.py'},
    ]
    data_pipeline_name = 'Create Lakehouse Tables'
    semantic_model_name = 'Product Sales DirectLake Model'
    reports = [
        { 'name': 'Product Sales Summary',
          'template':'product_sales_summary.json'},
        { 'name': 'Product Sales Time Intelligence',
          'template':'product_sales_time_intelligence.json'},
        { 'name': 'Product Sales Top 10 Cities',
           'template':'product_sales_top_ten_cities_report.json'}
    ]

    workspace = FabricRestApi.create_workspace(workspace_name)

    lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

    notebook_ids = {}
    for notebook_data in notebooks:
        create_notebook_request = \
        ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                        lakehouse,     \
                                                        notebook_data['name'], \
                                                        notebook_data['template'])
        notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
        notebook_ids[notebook['displayName']] = notebook['id']

    connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
        AppSettings.AZURE_STORAGE_SERVER,
        AppSettings.AZURE_STORAGE_PATH,
        workspace)

    pipeline_template = ItemDefinitionFactory.get_template_file(
        'DataPipelines//CreateLakehouseTables.json')

    pipeline_definition = \
        pipeline_template.replace('{WORKSPACE_ID}', workspace['id']) \
                        .replace('{LAKEHOUSE_ID}', lakehouse['id']) \
                        .replace('{CONNECTION_ID}', connection['id']) \
                        .replace('{CONTAINER_NAME}', AppSettings.AZURE_STORAGE_CONTAINER) \
                        .replace('{CONTAINER_PATH}', AppSettings.AZURE_STORAGE_CONTAINER_PATH) \
                        .replace('{NOTEBOOK_ID_BUILD_SILVER}', list(notebook_ids.values())[0]) \
                        .replace('{NOTEBOOK_ID_BUILD_GOLD}', list(notebook_ids.values())[1])

    create_pipeline_request = \
        ItemDefinitionFactory.get_data_pipeline_create_request(data_pipeline_name,
                                                               pipeline_definition)

    pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)
    FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

    sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

    create_model_request = \
        ItemDefinitionFactory.get_directlake_model_create_request(semantic_model_name,
                                                                'sales_model_DirectLake.bim',
                                                                sql_endpoint['server'],
                                                                sql_endpoint['database'])

    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    for report_data in reports:
        create_report_request = ItemDefinitionFactory.get_report_create_request(
            model['id'],
            report_data['name'],
            report_data['template'])

        FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_variable_library_solution():
    """Deploy variable library solution"""

    AppSettings.RUN_AS_SERVICE_PRINCIPAL = False
    FabricRestApi.authenticate()


    workspace_name = "Custom Variable Library Solution"
    lakehouse_name = "sales"
    notebooks = [
        { 'name': 'Create 01 Silver Layer', 'template':'BuildSilverLayer.py'},
        { 'name': 'Create 02 Gold Layer', 'template':'BuildGoldLayer.py'},
    ]
    data_pipeline_name = 'Create Lakehouse Tables'
    semantic_model_name = 'Product Sales DirectLake Model'
    reports = [
        { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'}
    ]

    AppLogger.log_job("Deploying Custom Variable Library solution")

    workspace = FabricRestApi.create_workspace(workspace_name)

    lakehouse = FabricRestApi.create_lakehouse(workspace['id'], lakehouse_name)

    notebook_ids = []
    for notebook_data in notebooks:
        create_notebook_request = \
        ItemDefinitionFactory.get_notebook_create_request(workspace['id'],  \
                                                        lakehouse,     \
                                                        notebook_data['name'], \
                                                        notebook_data['template'])
        notebook = FabricRestApi.create_item(workspace['id'], create_notebook_request)
        notebook_ids.append(notebook['id'])

    connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
        AppSettings.AZURE_STORAGE_SERVER,
        AppSettings.AZURE_STORAGE_PATH,
        workspace)

    variable_library = VariableLibrary()
    variable_library.add_variable("web_datasource_path", AppSettings.WEB_DATASOURCE_ROOT_URL)
    variable_library.add_variable("adls_server", AppSettings.AZURE_STORAGE_SERVER)
    variable_library.add_variable("adls_container_name",  AppSettings.AZURE_STORAGE_CONTAINER)
    variable_library.add_variable("adls_container_path",  AppSettings.AZURE_STORAGE_CONTAINER_PATH)
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
        ItemDefinitionFactory.get_data_pipeline_create_request(
            data_pipeline_name,
            pipeline_definition)

    pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)
    FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

    sql_endpoint = FabricRestApi.get_sql_endpoint_for_lakehouse(workspace['id'], lakehouse)

    create_model_request = \
        ItemDefinitionFactory.get_directlake_model_create_request(semantic_model_name,
                                                                'sales_model_DirectLake.bim',
                                                                sql_endpoint['server'],
                                                                sql_endpoint['database'])

    model = FabricRestApi.create_item(workspace['id'], create_model_request)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    for report_data in reports:
        create_report_request = \
            ItemDefinitionFactory.get_report_create_request(
                model['id'],
                report_data['name'],
                report_data['template'])

        FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")

def deploy_warehouse_solution():
    """Deploy Warehouse Solution"""

    WORKSPACE_NAME = "Custom Warehouse Solution"
    LAKEHOUSE_NAME = "staging"
    WAREHOUSE_NAME = "sales"

    DATA_PIPELINES = [
        { 'name': 'Load Tables in Staging Lakehouse', 'template':'LoadTablesInStagingLakehouse.json'},
        { 'name': 'Create Warehouse Tables', 'template':'CreateWarehouseTables.json'},
        { 'name': 'Create Warehouse Stored Procedures', 'template':'CreateWarehouseStoredProcedures.json'},
        { 'name': 'Refresh Warehouse Tables', 'template':'RefreshWarehouseTables.json'}
    ]

    SEMANTIC_MODEL_NAME = 'Product Sales DirectLake Model'
    REPORTS = [
        { 'name': 'Product Sales Summary', 'template':'product_sales_summary.json'},
        { 'name': 'Product Sales Time Intelligence', 'template':'product_sales_time_intelligence.json'},
        { 'name': 'Product Sales Top 10 Cities', 'template':'product_sales_top_ten_cities_report.json'},
    ]

    AppLogger.log_job("Deploying Warehouse solution")

    workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

    data_prep_folder = FabricRestApi.create_folder(workspace['id'], 'data_prep')
    data_prep_folder_id = data_prep_folder['id']

    lakehouse = FabricRestApi.create_lakehouse(
        workspace['id'],
        LAKEHOUSE_NAME,
        data_prep_folder_id)

    warehouse = FabricRestApi.create_warehouse(
        workspace['id'],
        WAREHOUSE_NAME
    )

    warehouse_connect_string = FabricRestApi.get_warehouse_connection_string(
            workspace['id'],
            warehouse['id'])

    connection = FabricRestApi.create_azure_storage_connection_with_sas_token(
        AppSettings.AZURE_STORAGE_SERVER,
        AppSettings.AZURE_STORAGE_PATH,
        workspace)

    for data_pipeline in DATA_PIPELINES:
        template_file = f"DataPipelines//{data_pipeline['template']}"
        template_content = ItemDefinitionFactory.get_template_file(template_file)
        template_content = template_content.replace('{WORKSPACE_ID}', workspace['id']) \
                                        .replace('{LAKEHOUSE_ID}', lakehouse['id']) \
                                        .replace('{WAREHOUSE_ID}', warehouse['id']) \
                                        .replace('{WAREHOUSE_CONNECT_STRING}', warehouse_connect_string) \
                                        .replace('{CONNECTION_ID}', connection['id'])
        

        pipeline_create_request = ItemDefinitionFactory.get_data_pipeline_create_request(
            data_pipeline['name'],
            template_content)
        
        pipeline = FabricRestApi.create_item(
            workspace['id'], 
            pipeline_create_request,
            data_prep_folder_id)

        FabricRestApi.run_data_pipeline(workspace['id'], pipeline)


    createModelRequest = \
        ItemDefinitionFactory.get_directlake_model_create_request(SEMANTIC_MODEL_NAME,
                                                                'sales_model_DirectLake.bim',
                                                                warehouse_connect_string,
                                                                warehouse['id'])

    model = FabricRestApi.create_item(workspace['id'], createModelRequest)

    FabricRestApi.create_and_bind_semantic_model_connecton(workspace, model['id'], lakehouse)

    for report_data in REPORTS:
        create_report_request = ItemDefinitionFactory.get_report_create_request(model['id'],
                                                                                report_data['name'],
                                                                                report_data['template'])
        report = FabricRestApi.create_item(workspace['id'], create_report_request)

    AppLogger.log_job_ended("Solution deployment complete")


## end
match os.getenv("SOLUTION_NAME"):

    case 'Custom Power BI Solution':
        deploy_powerbi_solution()

    case 'Custom Notebook Solution':
        deploy_notebook_solution()

    case 'Custom Shortcut Solution':
        deploy_shortcut_solution()

    case 'Custom Data Pipeline Solution':
        deploy_data_pipeline_solution()

    case 'Custom Variable Library Solution':
        deploy_variable_library_solution()

    case 'Custom Warehouse Solution':
        deploy_warehouse_solution()

    case 'Deploy All Solutions':
        deploy_powerbi_solution()
        deploy_notebook_solution()
        deploy_shortcut_solution()
        deploy_data_pipeline_solution()
        deploy_warehouse_solution()
        deploy_variable_library_solution()
