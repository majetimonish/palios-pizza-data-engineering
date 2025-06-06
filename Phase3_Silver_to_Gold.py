# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading from Silver Layer

# COMMAND ----------

silver_path = "s3a://palios-pizza-data/silver/"

orders_df = spark.read.parquet(silver_path + "orders/")
order_details_df = spark.read.parquet(silver_path + "order_details/")
pizzas_df = spark.read.parquet(silver_path + "pizzas/")
pizza_types_df = spark.read.parquet(silver_path + "pizza_types/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining all Tables

# COMMAND ----------

# Joining order_details with pizzas
order_items_enriched = (
    order_details_df
    .join(pizzas_df, on="pizza_id", how="inner")
    .join(pizza_types_df, on="pizza_type_id", how="inner")
)

# Joining with orders to get timestamp
orders_gold = (
    order_items_enriched
    .join(orders_df.select("order_id", "order_datetime"), on="order_id", how="inner")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Revenue and Time Dimensions

# COMMAND ----------

from pyspark.sql.functions import col, month, dayofweek, hour, round

orders_gold = (
    orders_gold
    .withColumn("revenue", round(col("price") * col("quantity"), 2))
    .withColumn("order_month", month("order_datetime"))
    .withColumn("order_day_of_week", dayofweek("order_datetime"))  # 1 = Sunday
    .withColumn("order_hour", hour("order_datetime"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Layer

# COMMAND ----------

gold_path = "s3a://palios-pizza-data/gold/"
orders_gold.write.mode("overwrite").parquet(gold_path + "orders_enriched/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Insight

# COMMAND ----------

display(orders_gold.select("order_datetime", "name", "size", "category", "quantity", "price", "revenue").limit(5))

# COMMAND ----------

