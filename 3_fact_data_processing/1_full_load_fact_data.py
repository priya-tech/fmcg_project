# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Projects/fmcg_project/1_setup/utilities

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f'/Volumes/fmcg/child_company_s3/s3_data/{data_source}'
landing_path = f'{base_path}/landing/'
processed_path = f'{base_path}/processed/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

df = spark.read.format('csv')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(f'{landing_path}/*.csv')\
    .withColumn('read_timestamp', current_timestamp())\
    .select('*', '_metadata.file_name', '_metadata.file_size')

df.show(10)

# COMMAND ----------

df.write.format('delta')\
    .option('delta.enableChangeDataFeed', True)\
    .mode('append')\
    .saveAsTable(f'{catalog}.{bronze_schema}.{dataœ_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving files from source to processed

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file_info in files:
  dbutils.fs.mv(file_info.path, f'{processed_path}/{file_info.name}', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

bronze_df = spark.sql(f'SELECT * FROM {catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

bronze_df.show(5)

# COMMAND ----------

silver_df = bronze_df.filter(col('order_qty').isNotNull())

silver_df = silver_df.withColumn('customer_id', when(col('customer_id').rlike('^[0-9]+$'), col('customer_id')).otherwise("999999").cast("string"))

silver_df = silver_df.withColumn('order_placement_date', regexp_replace(col('order_placement_date'), r"^[A-Za-z]+,\s*", ""))

silver_df = silver_df.withColumn('order_placement_date', coalesce(
    try_to_date("order_placement_date", "yyyy/MM/dd"),
    try_to_date("order_placement_date", "dd-MM-yyyy"),
    try_to_date("order_placement_date", "dd/MM/yyyy"),
    try_to_date("order_placement_date", "MMMM dd, yyyy"),
))

silver_df = silver_df.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

silver_df = silver_df.withColumn('product_id', col('product_id').cast('string'))

# COMMAND ----------

silver_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Join with products**

# COMMAND ----------

df_products = spark.table('fmcg.silver.products')

joined_df = silver_df.join(df_products, ['product_id'], 'inner').select(silver_df['*'], df_products['product_code'])

# COMMAND ----------

joined_df.show(5)

# COMMAND ----------

silver_table = f'{catalog}.{silver_schema}.{data_source}'
if not spark.catalog.tableExists(silver_table):
    joined_df.write.format('delta').mode('overwrite').option('mergeSchema', True).option("delta.enableChangeDataFeed", "true").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias('target').merge(joined_df.alias('source'), 'target.order_id = source.order_id and target.order_placement_date = source.order_placement_date and target.customer_id = source.customer_id and target.product_id = source.product_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

gold_df = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")
gold_df.show(5)

# COMMAND ----------

gold_table = f'{catalog}.{gold_schema}.sb_fact_{data_source}'

if not spark.catalog.tableExists(gold_table):
    gold_df.write.format('delta').mode('overwrite').option('delta.enableChangeDataFeed','true').option('mergeSchema', True).saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias('target').merge(gold_df.alias('source'), 'source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging with Parent company

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

df_child = df_child.withColumn('month_start', trunc('date', 'MM'))\
                    .groupBy('month_start', 'product_code', 'customer_code').agg(sum('sold_quantity').alias('sold_quantity'))\
                    .withColumnRenamed('month_start', 'date')

display(df_child.limit(5))

# COMMAND ----------

df_child.count()

# COMMAND ----------

gold_parent = DeltaTable.forName(spark, f'{catalog}.{gold_schema}.fact_orders')
gold_parent.alias('target').merge(df_child.alias('source'), 'source.date = target.date AND source.product_code = target.product_code AND source.customer_code = target.customer_code').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

