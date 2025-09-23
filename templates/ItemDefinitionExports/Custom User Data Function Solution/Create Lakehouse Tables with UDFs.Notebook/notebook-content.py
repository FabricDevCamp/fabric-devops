# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ec08f4d6-0f38-4a61-8d08-6db84f6c20f1",
# META       "default_lakehouse_name": "sales",
# META       "default_lakehouse_workspace_id": "df219ee5-1c02-4c87-a748-0825f7fee416",
# META       "known_lakehouses": [
# META         {
# META           "id": "ec08f4d6-0f38-4a61-8d08-6db84f6c20f1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import notebookutils.udf

csv_base_url = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Test/'

sales_data_transforms = notebookutils.udf.getFunctions('sales_data_transforms')

output = sales_data_transforms.ingest_csv_files(url=csv_base_url)

print(f"Output: {output}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
