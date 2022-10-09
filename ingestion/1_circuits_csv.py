# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestion Circuits CSV files by using Datafram API

# COMMAND ----------

# MAGIC 
# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# Defining the Schema

circuit_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                  StructField("circuitRef",StringType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("location",StringType(),True),
                                  StructField("country",StringType(),True),
                                  StructField("lat",DoubleType(),True),
                                  StructField("lng",DoubleType(),True),
                                  StructField("alt",DoubleType(),True),
                                  StructField("url",StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-1 Reading the file by using reader API

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.schema(circuit_schema)\
.csv(f"{raw_path}/circuits.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step-2 Selecting the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Renaming the columns

# COMMAND ----------

circuit_renamed_df=circuit_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuitRef")\
.withColumnRenamed("lat","latitiude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step-4 Adding one new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuit_final_df=circuit_renamed_df.withColumn("ingestion_date",current_timestamp())\
# .withColumn("env",lit("prod"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step -5 Writing the data into parquet files

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet(f"{processed_path}/processed/circuits")

# COMMAND ----------


df=spark.read.parquet("/mnt/formuladl28/processed/processed/circuits")
display(df)