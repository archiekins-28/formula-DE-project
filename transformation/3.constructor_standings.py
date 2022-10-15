# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading the data from storage

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

results_df=spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when,desc,rank

# COMMAND ----------

final_df=results_df\
.groupBy('race_year','team')\
.agg(sum('points').alias('total_points'),
    count(when(col('position')==1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

rank_spec=Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))

# COMMAND ----------

constructor_findings=final_df.withColumn('rank',rank().over(rank_spec))

# COMMAND ----------

constructor_findings.write.mode('overwrite').parquet(f'{presentation_path}/constructor_standings')

# COMMAND ----------

display(spark.read.parquet(f'{presentation_path}/constructor_standings').filter('race_year=2020'))