"""Deploy Power BI Solution with Parameters"""

from fabric_devops import FabricRestApi, ItemDefinitionFactory, AppLogger, \
                          SampleCustomerData, DeploymentJob

deploy_job = SampleCustomerData.get_adventureworks()

WORKSPACE_NAME = deploy_job.target_workspace_name
SEMANTIC_MODEL_NAME = 'Product Sales Imported Model'
REPORT_NAME = 'Product Sales Summary'

AppLogger.log_job(f"Deploying Customer Power BI Solution to [{WORKSPACE_NAME}]")

deploy_job.display_deployment_parameters()

workspace = FabricRestApi.create_workspace(WORKSPACE_NAME)

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

report = FabricRestApi.create_item(workspace['id'], create_report_request)

AppLogger.log_job_ended("Solution deployment complete")
