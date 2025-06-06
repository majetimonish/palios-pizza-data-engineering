# Databricks notebook source
# MAGIC %md
# MAGIC ##Reading Data from Bronze Layer

# COMMAND ----------

bronze_path = "s3a://palios-pizza-data/bronze/"

orders_df = spark.read.parquet(bronze_path + "orders/")
order_details_df = spark.read.parquet(bronze_path + "order_details/")
pizzas_df = spark.read.parquet(bronze_path + "pizzas/")
pizza_types_df = spark.read.parquet(bronze_path + "pizza_types/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning Data

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Cleaning orders_df

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, to_timestamp

orders_df_clean = (
    orders_df
    .withColumn("order_datetime", to_timestamp(concat_ws(" ", col("date"), col("time")), "M/d/yy H:mm:ss"))
    .dropna(subset=["order_id", "order_datetime"])
    .dropDuplicates(["order_id", "order_datetime"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Cleaning order_details_df 

# COMMAND ----------

order_details_df_clean = (
    order_details_df
    .dropna(subset=["order_id", "pizza_id", "quantity"])
    .withColumn("quantity", col("quantity").cast("int"))
    .dropDuplicates()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Cleaning pizzas_df

# COMMAND ----------

from pyspark.sql.functions import upper, trim

pizzas_df_clean = (
    pizzas_df
    .dropna(subset=["pizza_id", "price", "size"])
    .withColumn("price", col("price").cast("double"))
    .withColumn("size", upper(trim(col("size"))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Cleaning pizza_types_df

# COMMAND ----------

pizza_types_df_clean = (
    pizza_types_df
    .dropna(subset=["pizza_type_id", "name"])
    .withColumn("category", trim(col("category")))
    .withColumn("ingredients", trim(col("ingredients")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving to Silver Layer

# COMMAND ----------

silver_path = "s3a://palios-pizza-data/silver/"

orders_df_clean.write.mode("overwrite").parquet(silver_path + "orders/")
order_details_df_clean.write.mode("overwrite").parquet(silver_path + "order_details/")
pizzas_df_clean.write.mode("overwrite").parquet(silver_path + "pizzas/")
pizza_types_df_clean.write.mode("overwrite").parquet(silver_path + "pizza_types/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Output

# COMMAND ----------

display(spark.read.parquet(silver_path + "orders/").limit(5))
display(spark.read.parquet(silver_path + "order_details/").limit(5))
display(spark.read.parquet(silver_path + "pizzas/").limit(5))
display(spark.read.parquet(silver_path + "pizza_types/").limit(5))

# COMMAND ----------

