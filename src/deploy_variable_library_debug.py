"""Deploy Variable Solution"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger

WORKSPACE_NAME = "Custom Variable Library Solution"
LAKEHOUSE_NAME = "sales"
DATA_PIPELINE_NAME = 'Create Lakehouse Tables 3'

AppLogger.log_job("Deploying Custom Variable Library solution")

workspace = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)
lakehouse = FabricRestApi.get_item_by_name(workspace['id'], LAKEHOUSE_NAME, 'Lakehouse')
CONNECTION_NAME = 'Workspace[f5a74ace-baf6-4354-b33e-e4e8381f2117]-ADLS'
connection = FabricRestApi.get_connection_by_name(CONNECTION_NAME)


create_pipeline_request = \
    ItemDefinitionFactory.get_data_pipeline_create_request(DATA_PIPELINE_NAME)

pipeline = FabricRestApi.create_item(workspace['id'], create_pipeline_request)
FabricRestApi.run_data_pipeline(workspace['id'], pipeline)

AppLogger.log_job_ended("Solution deployment complete")
