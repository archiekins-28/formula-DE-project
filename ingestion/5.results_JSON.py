# Databricks notebook source
# MAGIC %md
# MAGIC #### Reading the files using spark API

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

# MAGIC %run "../utilities/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
p_data_source=dbutils.widgets.get('p_data_source')


# COMMAND ----------

dbutils.widgets.text("p_file_date","")
p_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

print(raw_path)
print(processed_path)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

# COMMAND ----------

results_schema=StructType(fields=([StructField('resultId',IntegerType(),True),\
                                   StructField('raceId',IntegerType(),False),\
                                   StructField('driverId',IntegerType(),False),\
                                   StructField('constructorId',IntegerType(),False),\
                                   StructField('number',IntegerType(),False),\
                                   StructField('grid',IntegerType(),False),\
                                   StructField('position',IntegerType(),False),\
                                   StructField('positionText',StringType(),False),\
                                   StructField('positionOrder',IntegerType(),False),\
                                   StructField('points',FloatType(),False),\
                                   StructField('laps',IntegerType(),False),\
                                   StructField('time',StringType(),False),\
                                   StructField('milliseconds',IntegerType(),False),\
                                   StructField('fastestLap',IntegerType(),False),\
                                   StructField('rank',IntegerType(),False),\
                                   StructField('fastestLapSpeed',StringType(),False),\
                                   StructField('fastestLapTime',StringType(),False),\
                                   StructField('statusId',IntegerType(),False),
                                   
    
]))

# COMMAND ----------

df_results=spark.read\
.schema(results_schema)\
.json(f'{raw_path}/inc-raw/{p_file_date}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping the status_id column

# COMMAND ----------

dropped_results_df=df_results.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns adding the ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_results_df=dropped_results_df.withColumnRenamed('resultId','result_id')\
.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('constructorId','constructor_id')\
.withColumnRenamed('positionText','position_text')\
.withColumnRenamed('positionOrder','position_order')\
.withColumnRenamed('fastestLap','fastest_lap')\
.withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
.withColumnRenamed('fastestLapTime','fastest_lap_time')\
.withColumn('ingestion_date',current_timestamp())\
.withColumn("data_source",lit(p_data_source))\
.withColumn("file_date",lit(p_file_date))\
.withColumnRenamed('raceId','race_id')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the data into the datalake

# COMMAND ----------

# final_results_df.write\
# .mode('overwrite')\
# .partitionBy('race_id')\
# .parquet(f'{processed_path}/processed/results')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1 to load the data incrementally 

# COMMAND ----------

# for race_ID_list in final_results_df.select('race_id').distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processsed.results"):
#         spark.sql(f"ALTER TABLE f1_processsed.results DROP IF EXISTS PARTITION (race_id={race_ID_list.race_id})")

# COMMAND ----------

# final_results_df.write\
# .mode('append')\
# .format('parquet')\
# .partitionBy('race_id')\
# .saveAsTable('f1_processsed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2 

# COMMAND ----------

spark.conf.set("spark.sql.partitionOverwriteMode","dynamic")

# COMMAND ----------


spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

final_results_df=final_results_df.select('result_id','driver_id','constructor_id','number','grid','position','position_text','position_order','points','laps','time','milliseconds','fastest_lap','rank','fastest_lap_speed','fastest_lap_time','ingestion_date','data_source','file_date','race_id')

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists("f1_processsed.results"):
    final_results_df.write.mode('overwrite').insertInto('f1_processsed.results')
else:
     final_results_df.write.mode('overwrite').format('parquet').partitionBy('race_id').saveAsTable('f1_processsed.results')  

# COMMAND ----------

#Incremental Load via function
incremental(final_results_df,'f1_processsed','results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   race_id,
# MAGIC   count(1)
# MAGIC FROM
# MAGIC   f1_processsed.results
# MAGIC GROUP BY
# MAGIC   race_id
# MAGIC ORDER by
# MAGIC   race_id DESC

# COMMAND ----------

# display(spark.read.parquet(f'{processed_path}/results'))

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processsed.results

# COMMAND ----------

partition_col='race_id'
col=[]

# COMMAND ----------


for i in final_results_df.schema.names:
    if i != partition_col:
        col.append(i)
col.append(partition_col)
print(col)