# Databricks notebook source
storage_account_name = "adlsaccountbayers"
container_name = "bayerhackthon17may2025"


sas_token = "sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-07-31T14:36:05Z&st=2025-05-30T06:36:05Z&spr=https&sig=pwjkim7fPrJPbHpXPjk2UF1t45jpbG94NWCP4Cm%2F%2FbI%3D"


source_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"


dbutils.fs.mount(
  source = source_url,
  mount_point = "/mnt/rakesh_hackathon_2025",
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/rakesh_hackathon_2025/SourceFiles")

# COMMAND ----------

csv_files = [file.path for file in dbutils.fs.ls("/mnt/rakesh_hackathon_2025/SourceFiles") if file.path.endswith(".csv")]

spark.sql("CREATE SCHEMA IF NOT EXISTS rakesh_bronze")

for csv_file in csv_files:
    table_name = csv_file.split("/")[-1].replace(".csv", "")
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    
    for col in df.columns:
        new_col = col.replace(" ", "_").replace(",", "_").replace(";", "_").replace("{", "_") \
                     .replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_") \
                     .replace("\t", "_").replace("=", "_")
        df = df.withColumnRenamed(col, new_col)
    
    df.write.mode("overwrite").saveAsTable(f"rakesh_bronze.{table_name}")

display(spark.sql("SHOW TABLES IN rakesh_bronze"))