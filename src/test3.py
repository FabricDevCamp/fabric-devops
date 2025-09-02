from fabric_devops import DeploymentManager, FabricRestApi, ItemDefinitionFactory

#workspace = FabricRestApi.create_workspace("Acme")
#FabricRestApi.provision_workspace_identity(workspace['id'])

workspace = FabricRestApi.get_workspace_by_name("Acme")

if FabricRestApi.workspace_has_provisioned_identity(workspace['id']):
    print("Workspace has a provisioned identity.")
else:
    print("Workspace does not have a provisioned identity.")
