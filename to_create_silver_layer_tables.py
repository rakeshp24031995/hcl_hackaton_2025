# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

bronze_customer_df = spark.table("rakesh_bronze.customer")

silver_customer_df = bronze_customer_df.dropna("Customer_Id").dropDuplicates()
spark.sql("CREATE SCHEMA IF NOT EXISTS rakesh_silver")

silver_customer_df.write.format("delta").mode("overwrite").saveAsTable("rakesh_silver.customer")

# COMMAND ----------

best way to use dropduplicates

# COMMAND ----------

sales_order_df = spark.table("rakesh_bronze.salesorder").select(F.col("SalesOrderId").alias("SalesOrderId_1"), "*").drop("_c10", "_c11", "SalesOrderId")

# COMMAND ----------

sales_orderline_df = spark.table("rakesh_bronze.salesorderline").select(F.col("salesOrderId").alias("SalesOrderId_2"), "*").drop("salesOrderId")

# COMMAND ----------

sales_order_join_df = sales_order_df.join(sales_orderline_df,sales_order_df.SalesOrderId_1 == sales_orderline_df.SalesOrderId_2 )

# COMMAND ----------

silver_sales_order_df = sales_order_join_df.dropna(how="SalesOrderId_1").dropDuplicates()

# COMMAND ----------

silver_sales_order_df.write.format("delta").mode("overwrite").saveAsTable("rakesh_silver.sales_orders")

# COMMAND ----------

sales_order_df = spark.table("rakesh_silver.sales_orders")

# COMMAND ----------

sales_order_payment_df = spark.table("rakesh_bronze.cardpayment").select(F.col("SalesOrderID").alias("SalesOrderId_3"), "*").drop("SalesOrderID")

# COMMAND ----------

sales_order_payment_join_df = sales_order_df.join(sales_order_payment_df,sales_order_df.SalesOrderId_1 == sales_order_payment_df.SalesOrderId_3, how="inner" )

# COMMAND ----------

sales_order_payment_join_df = sales_order_payment_join_df.dropna(how='SalesOrderId_1').dropDuplicates()

# COMMAND ----------

sales_order_payment_join_df.write.format("delta").mode("overwrite").saveAsTable("rakesh_silver.sales_orders_and_payments")

# COMMAND ----------

sales_order_df = spark.table("rakesh_bronze.returnorder").select(F.col("ReturnOrderId").alias("ReturnOrderId_1"), "*").drop("ReturnOrderId")

# COMMAND ----------

sales_order_df = spark.table("rakesh_bronze.rakesh_bronze.salesorderline").select(F.col("ReturnOrderId").alias("ReturnOrderId_1"), "*").drop("ReturnOrderId")