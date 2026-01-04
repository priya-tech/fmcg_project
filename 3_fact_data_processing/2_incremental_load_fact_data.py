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

# define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = spark.read.csv(f'{landing_path}/*.csv', header=True, inferSchema=True).withColumn('read_timestamp', current_timestamp()).select('*', '_metadata.file_name', '_metadata.file_size')

# COMMAND ----------

df.count()

# COMMAND ----------

df.write.format('delta')\
    .option('delta.enableChangeDataFeed', 'true')\
    .mode('append')\
    .saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just the arrived incremental data

# COMMAND ----------

df.write.format('delta')\
    .option('delta.enableChangeDataFeed', 'true')\
    .mode('overwrite')\
    .saveAsTable(f'{catalog}.{bronze_schema}.staging_{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving files from source to processed directory

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for f in files:
    dbutils.fs.mv(
      f.path,
      f'{processed_path}/{f.name}',
      True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

df_orders = spark.sql(f'SELECT * FROM {catalog}.{bronze_schema}.staging_{data_source}')
df_orders.show(5)

# COMMAND ----------

# 1. Keep only rows where order_qty is present
df_orders = df_orders.filter(col("order_qty").isNotNull())


# 2. Clean customer_id → keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    when(col("customer_id").rlike("^[0-9]+$"), col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# 3. Remove weekday name from the date text
#    "Tuesday, July 01, 2025" → "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    regexp_replace(col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    coalesce(
        try_to_date("order_placement_date", "yyyy/MM/dd"),
        try_to_date("order_placement_date", "dd-MM-yyyy"),
        try_to_date("order_placement_date", "dd/MM/yyyy"),
        try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 5. convert product id to string
df_orders = df_orders.withColumn('product_id', col('product_id').cast('string'))

# COMMAND ----------

df_orders.show(5)

# COMMAND ----------

df_orders.agg(
    min('order_placement_date'),
    max('order_placement_date')
).show()

# COMMAND ----------

df_products = spark.table('fmcg.silver.products')
df_joined = df_orders.join(df_products, on='product_id', how='inner').select(df_orders["*"], df_products["product_code"])

# COMMAND ----------

df_joined.show(5)

# COMMAND ----------

if not spark.catalog.tableExists(silver_table):
    df_joined.write.format('delta').mode('overwrite').option('delta.enableChangeDataFeed', 'true').option('mergeSchema', 'true').saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias('target').merge(
        df_joined.alias('source'), 'target.order_id = source.order_id and target.order_placement_date = source.order_placement_date and target.customer_id = source.customer_id and target.product_id = source.product_id'
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

df_joined.write.format('delta').mode('overwrite').option('delta.enableChangeDataFeed', 'true').option('mergeSchema', 'true').saveAsTable(f'{catalog}.{silver_schema}.staging_{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

df_gold.show(2)

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging with Parent company

# COMMAND ----------

df_child = spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")

incremental_month_df = df_child.select(
    trunc('date', 'MM').alias('start_month')
)

incremental_month_df.show(1)

incremental_month_df.createOrReplaceTempView('incremental_month')

# COMMAND ----------

child_history_df = spark.sql(f"""
                             SELECT date, product_code, customer_code, sold_quantity FROM {catalog}.{gold_schema}.sb_fact_orders sbf INNER JOIN incremental_month im ON trunc(sbf.date, 'MM') = im.start_month
                             """)

print("Total Rows: ", child_history_df.count())
child_history_df.show(10)                      

# COMMAND ----------

child_history_df.select('date').distinct().orderBy('date').show()

# COMMAND ----------

child_history_df = child_history_df.withColumn('month_start', trunc('date', 'MM'))\
                                .groupBy('month_start', 'product_code', 'customer_code')\
                                .agg(sum('sold_quantity').alias('sold_quantity'))\
                                .withColumnRenamed('month_start', 'date')

child_history_df.show(10)

# COMMAND ----------

child_history_df.count()

# COMMAND ----------

parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
parent_delta.alias('target').merge(
    child_history_df.alias('source'), 'target.date = source.date and target.product_code = source.product_code and target.customer_code = source.customer_code'
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.silver.staging_orders;

# COMMAND ----------

