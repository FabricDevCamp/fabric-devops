-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "{LAKEHOUSE_ID}",
-- META       "default_lakehouse_name": "{LAKEHOUSE_NAME}",
-- META       "default_lakehouse_workspace_id": "{WORKSPACE_ID}",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "{LAKEHOUSE_ID}"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- ## Create Lakehouse Schemas

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }