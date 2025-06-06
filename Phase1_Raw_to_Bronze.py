# Databricks notebook source
# MAGIC %md
# MAGIC Setting Up AWS S3 Access

# COMMAND ----------


spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<#ACCESS.KEY#>")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<#SECRET.KEY#>")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")

# COMMAND ----------

# MAGIC %md
# MAGIC Reading Raw CSV files from S3

# COMMAND ----------

raw_path = "s3a://palios-pizza-data/raw/"

orders_df = spark.read.option("header", True).csv(raw_path + "orders_2024_2025.csv")
order_details_df = spark.read.option("header", True).csv(raw_path + "order_details.csv")
pizzas_df = spark.read.option("header", True).csv(raw_path + "pizzas.csv")
pizza_types_df = spark.read.option("header", True).csv(raw_path + "pizza_types.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Preview the Data

# COMMAND ----------

orders_df.show(3)
order_details_df.show(3)
pizzas_df.show(3)
pizza_types_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Saving Parquet to Bronze Layer

# COMMAND ----------

bronze_path = "s3a://palios-pizza-data/bronze/"

orders_df.write.mode("overwrite").parquet(bronze_path + "orders/")
order_details_df.write.mode("overwrite").parquet(bronze_path + "order_details/")
pizzas_df.write.mode("overwrite").parquet(bronze_path + "pizzas/")
pizza_types_df.write.mode("overwrite").parquet(bronze_path + "pizza_types/")

# COMMAND ----------

# MAGIC %md
# MAGIC Validating Bronze Layer Output

# COMMAND ----------

display(spark.read.parquet(bronze_path + "orders/"))
display(spark.read.parquet(bronze_path + "order_details/"))
display(spark.read.parquet(bronze_path + "pizzas/"))
display(spark.read.parquet(bronze_path + "pizza_types/"))

# COMMAND ----------

