# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestion Races CSV files by using Datafram API

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

race_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                               StructField("year",IntegerType(),True),
                               StructField("circuitId",IntegerType(),True),
                               StructField("name",StringType(),True),
                               StructField("date",DateType(),True),
                               StructField("time",StringType(),True),
                               StructField("url",StringType(),True)
    
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step -1 Reading the race file by using Dataframe reader API

# COMMAND ----------

race_df=spark.read.option("header",True).csv("/mnt/formuladl28/raw/raw/races.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Selecting the requird columns

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

selected_race_df=race_df.select(col("raceId"),col("year"),col("circuitId"),col("name"),col("date"),col("time"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Adding new column and renaming the columns

# COMMAND ----------

# spark.sql ("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

pre_final_race_df=selected_race_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("year","race_year")\
.withColumnRenamed("circuitId","circuitId")\
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(" "),col('time')),'yyyy-mm-dd hh:mm:ss'))\
.withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping the unwanted columns

# COMMAND ----------

# df=pre_final_race_df.compute()
final_race_df=pre_final_race_df.drop('date','time')
display(final_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the data into the storage

# COMMAND ----------

final_race_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_path}/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formuladl28/processed/processed/races

# COMMAND ----------


display(spark.read.parquet("/mnt/formuladl28/processed/processed/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SET timezone = India;
# MAGIC SELECT current_timezone();
# MAGIC Select current_timestamp();