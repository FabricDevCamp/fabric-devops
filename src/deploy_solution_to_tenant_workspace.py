"""Deploy solution to tenant customer workspace"""
import os

from fabric_devops import DeploymentManager, SampleCustomerData, AppLogger

CUSTOMER_NAME = os.getenv("CUSTOMER_NAME")
SOLUTION_NAME = os.getenv("SOLUTION_NAME")

DEPLOYMENT_JOBS = []
match CUSTOMER_NAME:
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

    TARGET_WORKSPACE = f'Tenant - {DEPLOYMENT_JOB.name}'

    workspace = DeploymentManager.deploy_solution_by_name(
        SOLUTION_NAME,
        TARGET_WORKSPACE,
        DEPLOYMENT_JOB)

    AppLogger.log_job_complete(workspace.id)
