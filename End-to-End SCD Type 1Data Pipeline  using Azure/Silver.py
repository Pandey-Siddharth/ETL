# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('parquet')\
        .option('inferSchema',True)\
            .load('abfss://bronze@drousygingerdatalake.dfs.core.windows.net/raw_data')


# COMMAND ----------

# df.groupBy("Branch_ID","Dealer_ID","Model_ID","Date_ID").agg({"Revenue":"count"}).display()
df.display()

# COMMAND ----------

df = df.withColumn('model_category',split(col('model_id'),'-')[0]) 

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).printSchema() 

# COMMAND ----------

df = df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

df.write.format('parquet').mode('overwrite').save('abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@drousygingerdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

