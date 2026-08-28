# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11111111-1111-1111-1111-111111111111",
# META       "default_lakehouse_name": "sales",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "11111111-1111-1111-1111-111111111111",
# META           "workspace_id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs as labs

dataset_name = 'Product Sales DirectLake Model'

lakehouse_props = mssparkutils.lakehouse.lakehouse.getWithProperties()
lakehouse_name = lakehouse_props['displayName']
lakehouse_id = lakehouse_props['id']

print(f'Creating connection for dataset[{dataset}] to lakehouse[{lakehouse_name}]')

labs.directlake.update_direct_lake_model_lakehouse_connection(dataset_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
