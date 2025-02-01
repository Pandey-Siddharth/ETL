# Databricks notebook source

df_silver = spark.sql("select * from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales`")
df_silver.display()

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")
df_model = spark.sql("select * from cars_catalog.gold.dim_model")
df_date = spark.sql("select * from cars_catalog.gold.dim_date")
df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")

# COMMAND ----------

df_fact = df_silver.join(df_branch, df_silver.Branch_ID == df_branch.branch_id, how = "left").join(df_dealer, df_silver.Dealer_ID == df_dealer.dealer_id, how = "left").join(df_model, df_silver.Model_ID == df_model.model_id,how = "left").join(df_date, df_silver.Date_ID == df_date.date_id, how = "left").select(df_silver.Revenue,df_silver.Units_Sold,df_silver.RevPerUnit,df_dealer.dim_dealer_key,df_model.dim_model_key,df_date.dim_date_key,df_branch.dim_branch_key)

# COMMAND ----------

# # Read the existing table
# df_target = spark.table("cars_catalog.gold.fact_sales")

# # Deduplicate the target table based on the same keys
# df_target_dedup = df_target.dropDuplicates(["dim_dealer_key", "dim_model_key", "dim_date_key", "dim_branch_key"])

# # Overwrite the table with deduplicated data
# df_target_dedup.write.format("delta").mode("overwrite").saveAsTable("cars_catalog.gold.fact_sales")
# df_fact = df_fact.dropDuplicates(["dim_dealer_key", "dim_model_key", "dim_date_key", "dim_branch_key"])


# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.gold.fact_sales"):
    deltaTable = DeltaTable.forName(spark,"cars_catalog.gold.fact_sales")
    deltaTable.alias("t").merge(df_fact.alias("s"),"s.dim_dealer_key == t.dim_dealer_key and s.dim_model_key == t.dim_model_key and s.dim_date_key == t.dim_date_key and s.dim_branch_key == t.dim_branch_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fact.write.mode("overwrite").option("path","abfss://gold@drousygingerdatalake.dfs.core.windows.net/fact_sales").saveAsTable("cars_catalog.gold.fact_sales")

# COMMAND ----------

display(spark.sql( "select * from cars_catalog.gold.fact_sales"))

# COMMAND ----------

