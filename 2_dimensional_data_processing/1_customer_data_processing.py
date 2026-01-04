# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Projects/fmcg_project/1_setup/utilities

# COMMAND ----------

print(bronze_schema)

# COMMAND ----------

dbutils.widgets.text('catalog', 'fmcg', 'Catalog')
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

base_path = f'/Volumes/fmcg/child_company_s3/s3_data/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (
   spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

display(df.limit(10))

# COMMAND ----------

df.write.format('delta')\
       .mode('overwrite')\
       .option('delta.enableChangeDataFeed', True)\
       .saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver

# COMMAND ----------

bronze_df = spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source};")
bronze_df.show(10)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Drop Duplicates

# COMMAND ----------

duplicate_df = bronze_df.groupBy('customer_id').count().filter(col('count') > 1)
display(duplicate_df)

# COMMAND ----------

print('Rows before duplicates dropped: ', bronze_df.count())
silver_df = bronze_df.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', silver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Trim spaces in customer name

# COMMAND ----------

silver_df = silver_df.withColumn('customer_name', trim(col('customer_name')))

# COMMAND ----------

# MAGIC %md
# MAGIC - 3: Data Quality Fix: Correcting City Typos

# COMMAND ----------

silver_df.select('city').distinct().show()

# COMMAND ----------

# typos → correct names
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}


allowed = ["Bengaluru", "Hyderabad", "New Delhi"]


silver_df = silver_df.replace(city_mapping, subset=['city'])\
                        .withColumn('city', when(col('city').isNull(), None).when(col('city').isin(allowed), col('city')).otherwise(None))

# COMMAND ----------

silver_df.select('city').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 4: Fix Title-Casing Issue

# COMMAND ----------

silver_df.select('customer_name').distinct().show()

# COMMAND ----------

silver_df = silver_df.withColumn('customer_name', when(col('customer_name').isNull(), None)\
                    .otherwise(initcap(col('customer_name'))))

# COMMAND ----------

# MAGIC %md
# MAGIC - 5: Handling missing cities

# COMMAND ----------

silver_df.filter(col('city').isNull()).show()

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
silver_df.filter(col('customer_name').isin(null_customer_names)).show()


# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(i,j) for i,j in customer_city_fix.items() ],
    ['customer_id', 'fixed_city']
)

# COMMAND ----------

silver_df = silver_df.join(df_fix, 'customer_id', 'left')\
                    .withColumn('city', coalesce('city', 'fixed_city'))\
                    .drop('fixed_city')

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - 6: Convert customer_id to string

# COMMAND ----------

silver_df = silver_df.withColumn('customer_id', col('customer_id').cast('string'))
silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardizing Customer Attributes to Match Parent Company Data Model

# COMMAND ----------

silver_df = silver_df.withColumn('customer', concat_ws('-', col('customer_name'), coalesce(col('city'), lit('unknown')))).withColumn("market", lit("India"))\
  .withColumn("platform", lit("Sports Bar"))\
    .withColumn("channel", lit("Acquisition"))

# COMMAND ----------

silver_df.write.format('delta')\
    .mode('overwrite')\
    .option('delta.enableChangeDataFeed', True)\
    .option('mergeSchema', True)\
    .saveAsTable(f'{catalog}.{silver_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

silver_df = spark.sql(f'select * from {catalog}.{silver_schema}.{data_source}')

gold_df = silver_df.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

gold_df.write.format('delta')\
    .mode('overwrite')\
    .option('delta.enableChangeDataFeed', True)\
    .saveAsTable(f'{catalog}.{gold_schema}.sb_dim_{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging data source with parent

# COMMAND ----------

parent_df = DeltaTable.forName(spark, "fmcg.gold.dim_customers")

child_df = spark.table('fmcg.gold.sb_dim_customers').select(
    col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

parent_df.alias('target').merge(
    source=child_df.alias('source'),
    condition='target.customer_code = source.customer_code'
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

