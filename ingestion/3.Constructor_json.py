# Databricks notebook source
# MAGIC %md
# MAGIC #### Reading the Costructor file using Spark API

# COMMAND ----------

# MAGIC %run  "../utilities/parameters_NB"

# COMMAND ----------

constructor_schema='constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING'

# COMMAND ----------

costructor_df=spark.read.schema(constructor_schema).json(f'{raw_path}/constructors.json')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping the unwanted columns 

# COMMAND ----------

costructor_dropped_df=costructor_df.drop('url')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns and adding the new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_constructor_df=costructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
.withColumnRenamed('constructorRef','constructor_ref')\
.withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the data into the datalake

# COMMAND ----------

final_constructor_df.write.mode('overwrite').parquet(f'{processed_path}/processed/constructor')

# COMMAND ----------

display(spark.read.parquet(f'{processed_path}/processed/constructor'))