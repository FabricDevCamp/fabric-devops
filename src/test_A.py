import json
from fabric_devops import FabricRestApi

connectors = FabricRestApi.list_supported_connection_types()

for connector in connectors:
  if 'CosmosDB' in connector['type']:
    print( json.dumps(connector, indent=4) )


