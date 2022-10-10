# Databricks notebook source
# MAGIC %md
# MAGIC #### Reading the files using spark API

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

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
.json(f'{raw_path}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping the status_id column

# COMMAND ----------

dropped_results_df=df_results.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns adding the ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_results_df=dropped_results_df.withColumnRenamed('resultId','result_id')\
.withColumnRenamed('raceId','race_id')\
.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('constructorId','constructor_id')\
.withColumnRenamed('positionText','position_text')\
.withColumnRenamed('positionOrder','position_order')\
.withColumnRenamed('fastestLap','fastest_lap')\
.withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
.withColumnRenamed('fastestLapTime','fastest_lap_time')\
.withColumn('ingestion_date',current_timestamp())
display(renamed_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the data into the datalake

# COMMAND ----------

final_results_df.write\
.mode('overwrite')\
.partitionBy('race_id')\
.parquet(f'{processed_path}/processed/results')

# COMMAND ----------

display(spark.read.parquet(f'{processed_path}/processed/results'))