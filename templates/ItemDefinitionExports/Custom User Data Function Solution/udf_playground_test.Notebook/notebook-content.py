# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import notebookutils.udf

# Get the UDF item
udf_playground = notebookutils.udf.getFunctions('udf_playground')

message = "Bob's your unkle"

udf_playground.write_text_file(message)

message = udf_playground.read_text_file()

print( message )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output = udf_playground.get_udf_context()

print (output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
