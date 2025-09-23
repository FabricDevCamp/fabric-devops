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

# Get the UDF item
udf_playground = notebookutils.udf.getFunctions('udf_playground')

message = "Bob's your unkle"

udf_playground.write_text_file(message)

message = udf_playground.read_text_file()

print( message )

# CELL ********************

output = udf_playground.get_udf_context()

print (output)
