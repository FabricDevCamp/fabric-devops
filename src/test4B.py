
from fabric_devops import AdoProjectManager


project_name = "Angie"

project = AdoProjectManager.create_project(project_name)

AdoProjectManager.create_branch(project_name, 'test', 'main')
AdoProjectManager.create_branch(project_name, 'dev', 'test')
AdoProjectManager.set_default_branch(project_name, 'dev')

AdoProjectManager.copy_files_from_folder_to_repo(project_name, 'dev', 'ADO_SetupForFabricCICD')