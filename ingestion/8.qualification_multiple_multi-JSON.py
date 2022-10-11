# Databricks notebook source
# MAGIC %md
# MAGIC #### Read the files using Spark API

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

qualify_schema=StructType(fields=[StructField('qualifyId',IntegerType(),True),\
                                  StructField('raceId',IntegerType(),True),\
                                  StructField('driverId',IntegerType(),True),\
                                  StructField('constructorId',IntegerType(),True),\
                                  StructField('number',IntegerType(),False),\
                                  StructField('position',IntegerType(),False),\
                                  StructField('q1',StringType(),False),\
                                  StructField('q2',StringType(),False),\
                                  StructField('q3',StringType(),False)
    
])

# COMMAND ----------

qualify_df=spark.read\
.option('multiline',True)\
.schema(qualify_schema)\
.json(f'{raw_path}/qualifying/qualifying_split*.json')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ####  Renaming and adding one column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_qualify_df=qualify_df.withColumnRenamed('qualifyId','qualify_id')\
.withColumnRenamed('race_Id','race_id')\
.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('constructorId','constructor_id')\
.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

display(final_qualify_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the data into the storage

# COMMAND ----------

final_qualify_df.write.mode('overwrite').parquet(f'{processed_path}/processed/qualifying')

# COMMAND ----------

display(spark.read.parquet(f'{processed_path}/processed/qualifying'))