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

-- ## Create materialized lake views


-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.products AS
   SELECT
       product_id,
       product,
       category
    FROM
       silver.products;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.customers AS
    SELECT
       customer_id,
       country,
       city,
       dob,
       first_name + last_name AS customer,
       YEAR(CURRENT_TIMESTAMP) - YEAR(dob) AS age
    FROM
       silver.customers;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.sales AS
    SELECT 
        i.date,
        d.quantity,
        d.sales_amount,
        d.invoice_id,
        d.product_id,
        i.customer_id
    FROM silver.invoice_details AS d
    JOIN silver.invoices AS i
        ON d.invoice_id = i.invoice_id

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from datetime import date
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.functions import concat_ws, floor, datediff, current_date, col
-- MAGIC from pyspark.sql.functions import to_date, year, month, dayofmonth, quarter, dayofweek, date_format
-- MAGIC 
-- MAGIC first_sales_date = spark.sql("SELECT MIN(date) FROM sales.gold.sales").collect()[0][0]
-- MAGIC last_sales_date = spark.sql("SELECT MAX(date) FROM sales.gold.sales").collect()[0][0]
-- MAGIC 
-- MAGIC # calculate start date and end date for calendar table
-- MAGIC start_date = date(first_sales_date.year, 1, 1)
-- MAGIC end_date = date(last_sales_date.year, 12, 31)
-- MAGIC 
-- MAGIC # create pandas DataFrame with Date series column
-- MAGIC df_calendar_ps = pd.date_range(start_date, end_date, freq='D').to_frame()
-- MAGIC 
-- MAGIC # convert pandas DataFrame to Spark DataFrame and add calculated calendar columns
-- MAGIC df_calendar_spark = (
-- MAGIC      spark.createDataFrame(df_calendar_ps)
-- MAGIC          .withColumnRenamed("0", "timestamp")
-- MAGIC          .withColumn("date", to_date(col('timestamp')))
-- MAGIC          .withColumn("date_key", (year(col('timestamp'))*10000) + 
-- MAGIC                                 (month(col('timestamp'))*100) + 
-- MAGIC                                 (dayofmonth(col('timestamp')))     )
-- MAGIC          .withColumn("year", year(col('timestamp'))    )
-- MAGIC          .withColumn("quarter", date_format(col('timestamp'),"yyyy-QQ")    )
-- MAGIC          .withColumn("month", date_format(col('timestamp'),'yyyy-MM')    )
-- MAGIC          .withColumn("day", dayofmonth(col('timestamp'))    )
-- MAGIC          .withColumn("month_in_year", date_format(col('timestamp'),'MMMM')    )
-- MAGIC          .withColumn("month_in_year_sort", month(col('timestamp'))    )
-- MAGIC          .withColumn("day_of_week", date_format(col('timestamp'),'EEEE')    )
-- MAGIC          .withColumn("day_of_week_sort", dayofweek(col('timestamp')))
-- MAGIC          .drop('timestamp')
-- MAGIC )
-- MAGIC 
-- MAGIC # write DataFrame to new gold layer table 
-- MAGIC ( df_calendar_spark.write
-- MAGIC                      .mode("overwrite")
-- MAGIC                      .option("overwriteSchema", "True")
-- MAGIC                      .format("delta")
-- MAGIC                      .saveAsTable("gold.calendar")
-- MAGIC )
-- MAGIC 
-- MAGIC # display table schema and data
-- MAGIC df_calendar_spark.printSchema()
-- MAGIC df_calendar_spark.show()

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
