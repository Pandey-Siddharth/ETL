# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

df_src = spark.sql('select distinct(branch_id) as branch_id,BranchName from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales`')


# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink = spark.sql('select dim_branch_key,branch_id,BranchName from cars_catalog.gold.dim_branch')
else:
    df_sink = spark.sql('select 1 as dim_branch_key,branch_id,BranchName from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales` where 1=0')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.branch_id == df_sink.branch_id, 'left').select(df_src.branch_id,df_src.BranchName,df_sink.dim_branch_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_branch_key.isNotNull())

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_branch_key.isNull()).select(df_src.branch_id,df_src.BranchName)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

if dbutils.widgets.get('incremental_flag') == '0':
    max_value = 1
    # dbutils.widgets.text('incremental_flag', '1')
else:
    max_value = spark.sql("select max(dim_branch_key) from cars_catalog.gold.dim_branch").collect()[0][0]

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_branch_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

print("hello")

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@drousygingerdatalake.dfs.core.windows.net/cars_catalog/dim_branch")
    delta_tbl.alias("tg").merge(df_final.alias("sr"),"tg.branch_id == sr.branch_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final.write.format("delta").mode("overwrite").option("path","abfss://gold@drousygingerdatalake.dfs.core.windows.net/cars_catalog/dim_branch").saveAsTable('cars_catalog.gold.dim_branch')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch

# COMMAND ----------

