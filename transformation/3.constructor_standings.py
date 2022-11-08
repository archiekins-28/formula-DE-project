# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading the data from storage

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

# MAGIC %run "../utilities/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Finding the years to be processed

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
p_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_df_list=spark.read.parquet(f'{presentation_path}/Incremental/race_results')\
.filter(f"file_date='{p_file_date}'")\
.select('race_year')\
.distinct()\
.collect()


# COMMAND ----------

results_df_list

# COMMAND ----------

column_name='race_year'

# COMMAND ----------

row[column_name]

# COMMAND ----------

column_value_list = [row[column_name] for row in results_df_list]
print(column_value_list)

# COMMAND ----------

race_year_list=[]
for race_year in results_df_list:
    race_year_list.append(race_year.race_year)
print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when,desc,rank

# COMMAND ----------

results_df=spark.read.parquet(f'{presentation_path}/Incremental/race_results')\
.filter(col('race_year').isin(race_year_list))

# COMMAND ----------

display(results_df)

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

# constructor_findings.write.mode('overwrite').parquet(f'{presentation_path}/constructor_standings')

# COMMAND ----------

constructor_findings.write.mode('overwrite').format('parquet').saveAsTable('f1_present_prod.constructor_standings')

# COMMAND ----------

incremental (constructor_findings,'f1_presentation','constructor_standings','race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year,count(*) FROM f1_presentation.constructor_standings GROUP BY 1 ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings 