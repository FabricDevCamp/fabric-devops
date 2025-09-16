
from fabric_devops import AdoProjectManager


PROJECT_NAME = "Apollo"

AdoProjectManager.copy_files_from_folder_to_repo(
    PROJECT_NAME, 
    'dev', 
    'ADO_SetupForFabricCICD'
)

AdoProjectManager.create_and_merge_pull_request(PROJECT_NAME, 'dev','test')
AdoProjectManager.create_and_merge_pull_request(PROJECT_NAME, 'test','main')
