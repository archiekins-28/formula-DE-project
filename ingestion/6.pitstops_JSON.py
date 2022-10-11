# Databricks notebook source
# MAGIC %md
# MAGIC #### Reading the file using the reader API

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

pitstops_schema=StructType(fields=[StructField('race_id',IntegerType(),True),\
                                   StructField('driverId',IntegerType(),True),\
                                   StructField('stop',IntegerType(),True),\
                                   StructField('lap',IntegerType(),False),\
                                   StructField('time',StringType(),False),\
                                   StructField('duration',StringType(),False),\
                                   StructField('milliseconds',IntegerType(),False)
    
])

# COMMAND ----------

pitstops_df=spark.read\
.schema(pitstops_schema)\
.option('multiline',True)\
.json(f'{raw_path}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming and adding new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------


final_pitstops_df=pitstops_df.withColumnRenamed('raceId','race_id')\
.withColumnRenamed('driverId','driver_id')\
.withColumn('ingestion_date',current_timestamp())
display(final_pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the data into the data storage

# COMMAND ----------

final_pitstops_df.write.mode('overwrite').parquet(f'{processed_path}/processed/pitstops')

# COMMAND ----------

display(spark.read.parquet(f'{processed_path}/pitstops'))