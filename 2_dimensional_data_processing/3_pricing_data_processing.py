# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Workspace/Projects/fmcg_project/1_setup/utilities

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f'/Volumes/fmcg/child_company_s3/s3_data/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze

# COMMAND ----------

df = spark.read.format('csv')\
          .option("header", True)\
          .option("inferSchema", True)\
          .load(base_path)\
          .withColumn('read_timestamp', current_timestamp())\
          .select('*', '_metadata.file_name', '_metadata.file_size')

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

bronze_df = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
bronze_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Normalise `month` field

# COMMAND ----------

bronze_df.select('month').distinct().show()

# COMMAND ----------

silver_df = bronze_df.withColumn('month', 
    coalesce(
        try_to_date(col("month"), "yyyy/MM/dd"),
        try_to_date(col("month"), "dd/MM/yyyy"),
        try_to_date(col("month"), "yyyy-MM-dd"),
        try_to_date(col("month"), "dd-MM-yyyy")
                                     ))

# COMMAND ----------

silver_df.select('month').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Handling `gross_price`

# COMMAND ----------

silver_df = silver_df.withColumn('gross_price', 
                            when(col('gross_price').rlike(r'^-?\d+(\.\d+)?$'), when(col('gross_price').cast('double') < 0, -1 * col('gross_price').cast('double')).otherwise(col('gross_price').cast('double'))).otherwise(0))

# COMMAND ----------

silver_df.show(10)

# COMMAND ----------

# Performing an inner join with the products table to fetch the correct product_code for each product_id

products_df = spark.table('fmcg.silver.products')
df_joined = silver_df.join(products_df.select('product_id', 'product_code'), on='product_id', how='inner')
df_joined = df_joined.select("product_id", "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size")
df_joined.show(10)


# COMMAND ----------

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true")\
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold

# COMMAND ----------

silver_df = spark.sql(f'select * from {catalog}.{silver_schema}.{data_source}')

# COMMAND ----------

gold_df = silver_df.select("product_code", "month", "gross_price")
gold_df.show(5)

# COMMAND ----------

gold_df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging data source with parent

# COMMAND ----------

gold_price_df = spark.table('fmcg.gold.sb_dim_gross_price')
display(gold_price_df)

# COMMAND ----------

gold_price_df = gold_price_df.withColumn('year', year(col('month')))\
                        .withColumn('is_zero', when(col('gross_price') == 0, 1).otherwise(0))

# COMMAND ----------

gold_price_df.show(10)

# COMMAND ----------

gold_price_latest_df = gold_price_df.withColumn('rnk', row_number().over(Window.partitionBy('product_code', 'year').orderBy(col('is_zero'), col('month').desc()))).filter(col('rnk') == 1)

# COMMAND ----------

display(gold_price_latest_df)

# COMMAND ----------

gold_price_latest_df = gold_price_latest_df.select("product_code", "year", "gross_price").withColumnRenamed("gross_price", "price_inr").select("product_code", "price_inr", "year")

gold_price_latest_df = gold_price_latest_df.withColumn('year', col('year').cast('string'))

gold_price_latest_df.show(5)

# COMMAND ----------

parent_df = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")

parent_df.alias('target').merge(
    source=gold_price_latest_df.alias('source'),
    condition = 'target.product_code = source.product_code'
).whenMatchedUpdate(
    set = {
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()

# COMMAND ----------

