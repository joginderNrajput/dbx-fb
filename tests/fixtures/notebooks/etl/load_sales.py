# Databricks notebook source
# MAGIC %md
# MAGIC # Load sales
# MAGIC Reads raw sales files and writes the bronze and gold tables.

# COMMAND ----------

dbutils.widgets.text("run_date", "2024-01-01")
dbutils.widgets.dropdown("env", "dev", ["dev", "prod"])
run_date = dbutils.widgets.get("run_date")
env = env

# COMMAND ----------

# MAGIC %run ./common/setup

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
jdbc_password = dbutils.secrets.get(scope="kv-sales", key="sql-password")
raw = spark.read.format("csv").option("header", "true").load("/mnt/raw/sales/" + run_date)
dbutils.fs.mount(source="wasbs://raw@acct.blob.core.windows.net", mount_point="/mnt/other")
files = dbutils.fs.ls("/mnt/raw/sales")

# COMMAND ----------

raw.write.mode("overwrite").saveAsTable("main.bronze.sales_raw")
gold = spark.table("main.gold.daily_sales")
stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("dbfs:/mnt/raw/events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, sum(amount) FROM main.gold.daily_sales GROUP BY region

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /dbfs/tmp

# COMMAND ----------

result = dbutils.notebook.run("/Repos/team/etl/publish", 600, {"run_date": run_date})
dbutils.notebook.exit(result)
