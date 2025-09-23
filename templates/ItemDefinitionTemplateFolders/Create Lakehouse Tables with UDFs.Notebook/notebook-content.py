# Synapse Analytics notebook source

# METADATA ********************

# META {
# META     "synapse": {
# META     "lakehouse": {
# META         "default_lakehouse": "{LAKEHOUSE_ID}",
# META         "default_lakehouse_name": "{LAKEHOUSE_NAME}",
# META         "default_lakehouse_workspace_id": "{WORKSPACE_ID}",
# META         "known_lakehouses": [
# META         {
# META             "id": "{LAKEHOUSE_ID}"
# META         }
# META         ]
# META     }
# META     }
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
