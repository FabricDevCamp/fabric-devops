from fabric_devops import FabricRestApi, ItemDefinitionFactory

# FabricRestApi.get_workspace_by_name("Product Sales")

ItemDefinitionFactory.export_item_definitions_from_workspace("Product Sales")
