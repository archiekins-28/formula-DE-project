# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ####Reading the files using the reader API

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,StructField

# COMMAND ----------

lap_times_schema=StructType(fields=[StructField('race_id',IntegerType(),False),\
                                    StructField('driver_id',IntegerType(),False),\
                                    StructField('lap',IntegerType(),False),\
                                    StructField('position',IntegerType(),True),\
                                    StructField('time',StringType(),True),\
                                    StructField('milliseconds',IntegerType(),True)  
])

# COMMAND ----------

laptimes_df=spark.read.schema(lap_times_schema).csv(f'{raw_path}/lap_times/lap_times_split*.csv')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

final_laptime_df=laptimes_df.withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

final_laptime_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the data into the storage

# COMMAND ----------

final_laptime_df.write.mode('overwrite').parquet(f'{processed_path}/processed/lap_times')

# COMMAND ----------

display(spark.read.parquet(f'{processed_path}/processed/lap_times'))