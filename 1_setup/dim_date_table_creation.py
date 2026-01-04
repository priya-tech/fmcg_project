# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# start date and end date
start_date = "2024-01-01"
end_date   = "2025-12-01"

# COMMAND ----------

df = spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'), 
                to_date('{end_date}'), 
                interval 1 month)
        ) AS month_start_date
              """)

# COMMAND ----------

df = df.withColumn('date_key', date_format('month_start_date', 'yyyyMM').cast('int'))\
        .withColumn('year', year('month_start_date'))\
        .withColumn("month_name", date_format("month_start_date", "MMMM"))\
        .withColumn("month_short_name", date_format("month_start_date", "MMM"))\
        .withColumn('quarter', concat(lit('Q'), quarter('month_start_date')))\
        .withColumn("year_quarter", concat(col('year'), lit('-Q'), quarter('month_start_date')))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('fmcg.gold.dim_date')