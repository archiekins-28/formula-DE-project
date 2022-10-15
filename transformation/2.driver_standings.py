# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Reading the data from the storage

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

result_df=spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col,desc,rank

# COMMAND ----------

final_df=result_df\
.groupBy('driver_name','race_year','team','driver_location')\
.agg(sum('points').alias('total_points'),
    count(when(col('position')==1,True)).alias('wins'))
   

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driver_rank= Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))

# COMMAND ----------

driver_standings_df=final_df.withColumn('rank',rank().over(driver_rank))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Writing the data into the datalake

# COMMAND ----------

driver_standings_df.write.mode('overwrite').parquet(f'{presentation_path}/driver_standings')

# COMMAND ----------

display(spark.read.parquet(f'{presentation_path}/driver_standings'))