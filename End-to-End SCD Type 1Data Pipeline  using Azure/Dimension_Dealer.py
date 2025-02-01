# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

df_src = spark.sql('select distinct(dealer_id) as dealer_id,DealerName from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales`')


# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    df_sink = spark.sql('select dim_dealer_key,dealer_id,DealerName from cars_catalog.gold.dim_dealer')
else:
    df_sink = spark.sql('select 1 as dim_dealer_key,dealer_id,DealerName from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales` where 1=0')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.dealer_id == df_sink.dealer_id, 'left').select(df_src.dealer_id,df_src.DealerName,df_sink.dim_dealer_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_dealer_key.isNotNull())

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_dealer_key.isNull()).select(df_src.dealer_id,df_src.DealerName)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

if dbutils.widgets.get('incremental_flag') == '0':
    max_value = 1
    # dbutils.widgets.text('incremental_flag', '1')
else:
    max_value = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer").collect()[0][0]

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

print("hello")

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):   
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@drousygingerdatalake.dfs.core.windows.net/cars_catalog/dim_dealer")
    delta_tbl.alias("tg").merge(df_final.alias("sr"),"tg.dealer_id == sr.dealer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final.write.format("delta").mode("overwrite").option("path","abfss://gold@drousygingerdatalake.dfs.core.windows.net/cars_catalog/dim_dealer").saveAsTable('cars_catalog.gold.dim_dealer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer

# COMMAND ----------

