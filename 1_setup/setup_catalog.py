# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM fmcg;

# COMMAND ----------

