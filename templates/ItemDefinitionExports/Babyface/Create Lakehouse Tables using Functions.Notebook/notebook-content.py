# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

url = 'https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Test/'

transform_functions = notebookutils.udf.getFunctions('sales_data_transforms')

# Invoke the function
result = transform_functions.ingest_csv_files(url)

print( result )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
