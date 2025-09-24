"""Deploy Solution with Parameters"""
import os

from fabric_devops import DeploymentManager, EnvironmentSettings, SampleCustomerData, AppLogger, StagingEnvironments,\
                          AdoProjectManager, FabricRestApi, GitHubRestApi

if os.getenv("RUN_AS_SERVICE_PRINCIPAL") == 'true':
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = True
else:
    EnvironmentSettings.RUN_AS_SERVICE_PRINCIPAL = False

solution_name = os.getenv("SOLUTION_NAME")
workspace_name = solution_name


DEPLOYMENT_JOBS = []
match os.getenv("CUSTOMER_NAME"):
    case 'Adventure Works':
        DEPLOYMENT_JOBS = [SampleCustomerData.get_adventureworks()]
    case 'Contoso':
        CUSTOMER_JOBS = [SampleCustomerData.get_contoso()]
    case 'Fabrikam':
        DEPLOYMENT_JOBS = [SampleCustomerData.get_fabrikam()]
    case 'Northwind':
        DEPLOYMENT_JOBS = [SampleCustomerData.get_northwind()]
    case 'Seamark Farms':
        DEPLOYMENT_JOBS = [SampleCustomerData.get_seamarkfarms()]
    case 'Wingtip Toys':
        DEPLOYMENT_JOBS = [SampleCustomerData.get_wingtip()]
    case 'Deploy To All Customers':
        DEPLOYMENT_JOBS = SampleCustomerData.get_all_customers()

for DEPLOYMENT_JOB in DEPLOYMENT_JOBS:
    workspace_name = f'Tenant - {DEPLOYMENT_JOB.name}'
    workspace = DeploymentManager.deploy_solution_by_name(solution_name, workspace_name, DEPLOYMENT_JOB)
    AppLogger.log_job_complete(workspace['id'])

