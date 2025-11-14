"""Deploy Demo Solution with ADO GIT Intergation"""
import json
from fabric_devops import DeploymentManager, AppLogger, StagingEnvironments, \
                          AdoProjectManager, FabricRestApi, GitHubRestApi, ItemDefinitionFactory

create_notebook_request = \
            ItemDefinitionFactory.get_create_item_request_from_folder(
                'Create Lakehouse Tables.Notebook')
            
print( json.dumps(create_notebook_request, indent=4) )


