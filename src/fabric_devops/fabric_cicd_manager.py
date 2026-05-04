
import os
import time
from pathlib import Path
from azure.identity import ClientSecretCredential
from .app_logger import AppLogger
from fabric_cicd import FabricWorkspace, deploy_with_config, append_feature_flag, change_log_level
from .fabric_rest_api import FabricRestApi

change_log_level()

append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")
append_feature_flag("enable_shortcut_publish")
append_feature_flag("enable_lakehouse_unpublish")

class FabricCicdManager:
    """fabric-cicd Library Wrapper Class"""

    # Create a credential
    credential = ClientSecretCredential(
        tenant_id=os.getenv('AZURE_TENANT_ID'),
        client_id=os.getenv('AZURE_CLIENT_ID'),
        client_secret=os.getenv('AZURE_CLIENT_SECRET')
    )

    @classmethod
    def deploy(cls, solution_name: str, deploy_target: str = "dev", workspace_id: str = None):
        """Deploy solution using fabric-cicd library"""
        
        if deploy_target in ['dev', 'test', 'prod']:
            environment = deploy_target
        else:
            environment = 'prod'

        config_file_path = f".//templates//FabricSolutions//{solution_name}/deploy.yml"
        
        AppLogger.log_step(f'Starting fabric-cicd deployment using {config_file_path}')
        AppLogger.log_raw_text("-" * 120)
        time.sleep(1)
        
        # exclude VariableLibrary from deployment
        config_override = { 
            "core": { 
                "workspace_id": { environment: workspace_id},
                "item_types_in_scope": [ "Lakehouse", "Notebook", "DataPipeline", "SemanticModel", "Report"]
            }
        }

        deploy_with_config(
            config_file_path=config_file_path,
            environment=environment,
            config_override=config_override,
            token_credential=cls.credential
        )
                
        AppLogger.log_step_complete()
        

    @classmethod
    def deploy_to_existing_workspace(cls, solution_name: str, target_workspace: str, environment: str = "dev"):
        """Deploy solution to existing workspace using fabric-cicd library"""
        
        workspace = FabricRestApi.get_workspace_by_name(target_workspace)
        
        cls.deploy(
            solution_name, 
            environment,
            workspace.id
        )
        
        return workspace

    @classmethod
    def deploy_to_new_workspace(cls, solution_name: str, target_workspace: str, environment: str = "dev"):
        """Deploy solution to new workspace using fabric-cicd library"""
        
        workspace = FabricRestApi.create_workspace(target_workspace)
        
        cls.deploy(
            solution_name,
            environment,
            workspace.id
        )
        
        return workspace