-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Revenue report

-- COMMAND ----------

SET spark.databricks.delta.preview.enabled = true;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW revenue AS
SELECT * FROM main.gold.daily_sales VERSION AS OF 3;

-- COMMAND ----------

OPTIMIZE main.gold.daily_sales ZORDER BY (region);
