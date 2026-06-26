"""Setup project with fabric-cicd and release flow """
import json
import os

from fabric_devops import DeploymentManager

os.system("cls")

# workspace1 = DeploymentManager.import_from_solution_folder_to_new_workspace(
#     "Demo1", 
#     "Notebook Solution")

workspace1 = DeploymentManager.import_from_solution_folder_to_new_workspace(
    "Demo2",
    "Pipeline Solution")


# workspace1 = DeploymentManager.import_from_solution_folder_to_new_workspace(
#     "Howdy", 
#     "Shortcut Solution")


workspace6 = DeploymentManager.import_from_solution_folder_to_new_workspace(
    "Demo4", 
    "Medallion Solution")
