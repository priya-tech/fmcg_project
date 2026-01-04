# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Projects/fmcg_project/1_setup/utilities

# COMMAND ----------

print(bronze_schema)

# COMMAND ----------

dbutils.widgets.text('catalog', 'fmcg', 'Catalog')
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get('catalog')
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

base_path = f'/Volumes/fmcg/child_company_s3/s3_data/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

df = spark.read.format('csv')\
                    .option('header', True)\
                    .option('inferSchema', True)\
                    .load(base_path)\
                    .withColumn('read_timestamp', current_timestamp())\
                    .select('*', '_metadata.file_name', '_metadata.file_size')
                    

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write.format('delta')\
        .mode('overwrite')\
        .option('delta.enableChangeDataFeed','true')\
        .saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

bronze_df = spark.sql(f'select * from {catalog}.{bronze_schema}.{data_source}')
bronze_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Drop Duplicates

# COMMAND ----------

print('Rows before duplicates dropped: ', bronze_df.count())
silver_df = bronze_df.dropDuplicates(['product_id'])
print('Rows after duplicates dropped: ', silver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Title case fix

# COMMAND ----------

silver_df = silver_df.withColumn('category', 
                    when(col('category').isNull(), None).otherwise(initcap(col('category'))))

# COMMAND ----------

silver_df.select('category').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 3: Fix Spelling Mistake for `Protien`

# COMMAND ----------

silver_df = silver_df.withColumn('category', 
                    regexp_replace(col('category'), '(?i)protien', 'Protein'))\
                    .withColumn('product_name', 
                    regexp_replace(col('product_name'), '(?i)protien', 'Protein'))

# COMMAND ----------

display(silver_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing Customer Attributes to Match Parent Company Data Model

# COMMAND ----------

### Add Division column

silver_df = (
    silver_df
    .withColumn(
        "division",
        when(col("category") == "Energy Bars",        "Nutrition Bars")
         .when(col("category") == "Protein Bars",       "Nutrition Bars")
         .when(col("category") == "Granola & Cereals",  "Breakfast Foods")
         .when(col("category") == "Recovery Dairy",     "Dairy & Recovery")
         .when(col("category") == "Healthy Snacks",     "Healthy Snacks")
         .when(col("category") == "Electrolyte Mix",    "Hydration & Electrolytes")
         .otherwise("Other")
    )
)


### 2: Variant column
silver_df = silver_df.withColumn(
    "variant",
    regexp_extract(col("product_name"), r"\((.*?)\)", 1)
)

### 3: Create new column: product_code  
silver_df = silver_df.withColumn(
    "product_code", sha2(col('product_name').cast('string'), 256))\
                .withColumn('product_id', when(col('product_id').cast('string').rlike("^[0-9]+$"), col('product_id')).otherwise(lit(999999).cast('string')))\
                .withColumnRenamed('product_name', 'product')


# COMMAND ----------

silver_df = silver_df.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df.write.format('delta')\
        .mode('overwrite')\
        .option('delta.enableChangeDataFeed','true')\
        .option('mergeSchema','true')\
        .saveAsTable(f'{catalog}.{silver_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

silver_df = spark.sql(f'select * from {catalog}.{silver_schema}.{data_source}')
gold_df = silver_df.select("product_code", "product_id", "division", "category", "product", "variant")
gold_df.show(5)

# COMMAND ----------

gold_df.write.format('delta')\
        .option("delta.enableChangeDataFeed", "true") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging data source with Parent

# COMMAND ----------

parent_df = DeltaTable.forName(spark, "fmcg.gold.dim_products")
child_df = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")
child_df.show(5)

# COMMAND ----------

parent_df.alias('target').merge(
    source=child_df.alias('source'),
    condition='target.product_code = source.product_code'
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()