# Databricks notebook source
# MAGIC %md
# MAGIC #### Reading the Costructor file using Spark API

# COMMAND ----------

# MAGIC %run  "../utilities/parameters_NB"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
p_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
p_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructor_schema='constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING'

# COMMAND ----------

costructor_df=spark.read.schema(constructor_schema).json(f'{raw_path}/inc-raw/{p_file_date}/constructors.json')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping the unwanted columns 

# COMMAND ----------

costructor_dropped_df=costructor_df.drop('url')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns and adding the new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_constructor_df=costructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
.withColumnRenamed('constructorRef','constructor_ref')\
.withColumn('ingestion_date',current_timestamp())\
.withColumn("data_source",lit(p_data_source))\
.withColumn("file_date",lit(p_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the data into the datalake

# COMMAND ----------

# final_constructor_df.write.mode('overwrite').parquet(f'{processed_path}/processed/constructor')

# COMMAND ----------

final_constructor_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processsed.constructor')

# COMMAND ----------

display(spark.read.parquet(f'{processed_path}/constructor'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processsed.constructor

# COMMAND ----------

dbutils.notebook.exit('success')