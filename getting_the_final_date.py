# Databricks notebook source
sales_orders_and_payments_df = spark.table("rakesh_silver.sales_orders_and_payments")

top_5_customers_df = sales_orders_and_payments_df.groupBy("customerId") \
    .agg({"Amount": "sum"}) \
    .withColumnRenamed("sum(Amount)", "total_sales") \
    .orderBy("total_sales", ascending=False) \
    .limit(5)

display(top_5_customers_df)

# COMMAND ----------

top_5_products_df = sales_orders_and_payments_df.groupBy("ProductId") \
    .agg({"quantity": "sum"}) \
    .withColumnRenamed("sum(quantity)", "total_sales_products") \
    .orderBy("total_sales_products", ascending=False) \
    .limit(5)

display(top_5_products_df)

# COMMAND ----------

# sales_orders_and_payments_df = spark.table("rakesh_silver.sales_orders_and_payments")

# top_5_products_df = sales_orders_and_payments_df.groupBy("ProductId") \
#     .agg({"quantity": "sum"}) \
#     .withColumnRenamed("sum(quantity)", "total_sales_products") \
#     .orderBy("total_sales_products", ascending=False)

# COMMAND ----------

# spark.sql("CREATE SCHEMA IF NOT EXISTS rakesh_gold")

# top_5_products_df.write.format("delta").mode("overwrite").saveAsTable("rakesh_gold.products_sales_details")

# COMMAND ----------

