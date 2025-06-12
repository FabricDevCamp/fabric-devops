"""Demo 08 - Deploy Complete Fabric Solution"""

from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, FabricRestApi

AppLogger.clear_console()

AppLogger.log_job("Deploying Power BI Solution")

WORKSPACE_NAME = "Apollo-test"
WORKSPACE = FabricRestApi.get_workspace_by_name(WORKSPACE_NAME)
SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
SEMANTIC_MODEL = FabricRestApi.get_item_by_name(
    WORKSPACE['id'],
    SEMANTIC_MODEL_NAME,
    'SemanticModel')
DEPOYMENT_JOB = StagingEnvironments.get_test_environment()
WEB_DATASOURCE_PATH = DEPOYMENT_JOB.parameters[DEPOYMENT_JOB.web_datasource_path_parameter]

DeploymentManager.update_imported_semantic_model_source(
    WORKSPACE,
    SEMANTIC_MODEL_NAME,
    WEB_DATASOURCE_PATH)

FabricRestApi.create_and_bind_semantic_model_connecton(WORKSPACE, SEMANTIC_MODEL['id'])
