# Databricks notebook source
df = spark.table("main.gold.daily_sales")
df.write.mode("overwrite").saveAsTable("main.gold.daily_sales_published")
