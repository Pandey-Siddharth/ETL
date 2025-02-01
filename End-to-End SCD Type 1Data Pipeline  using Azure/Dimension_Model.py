# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

df_src = spark.sql('select distinct(model_id) as model_id,model_category from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales`')


# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql('select dim_model_key,model_id,model_category from cars_catalog.gold.dim_model')
else:
    df_sink = spark.sql('select 1 as dim_model_key,model_id,model_category from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales` where 1=0')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.model_id == df_sink.model_id, 'left').select(df_src.model_id,df_src.model_category,df_sink.dim_model_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_model_key.isNotNull())

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_model_key.isNull()).select(df_src.model_id,df_src.model_category)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

if dbutils.widgets.get('incremental_flag') == '0':
    max_value = 1
else:
    max_value = spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model").collect()[0][0]

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

print("hello")

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@drousygingerdatalake.dfs.core.windows.net/cars_catalog/dim_model")
    delta_tbl.alias("tg").merge(df_final.alias("sr"),"tg.model_id == sr.model_id").whenMatchedUpdateAll().whenNotMatchedInsertAll()
else:
    df_final.write.format("delta").mode("overwrite").option("path","abfss://gold@drousygingerdatalake.dfs.core.windows.net/cars_catalog/dim_model").saveAsTable('cars_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model

# COMMAND ----------

