# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestion Races CSV files by using Datafram API

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
p_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2022-03-21")
p_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

race_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                               StructField("year",IntegerType(),True),
                               StructField("round",IntegerType(),True),
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

race_df=spark.read.option("header",True).csv(f"{raw_path}/inc-raw/{p_file_date}/races.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Selecting the requird columns

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

selected_race_df=race_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Adding new column and renaming the columns

# COMMAND ----------

# spark.sql ("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

pre_final_race_df=selected_race_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("year","race_year")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(" "),col('time')),'yyyy-mm-dd hh:mm:ss'))\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(p_data_source))\
.withColumn("file_date",lit(p_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping the unwanted columns

# COMMAND ----------

# df=pre_final_race_df.compute()
final_race_df=pre_final_race_df.drop('date','time')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the data into the storage

# COMMAND ----------

# final_race_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_path}/processed/races")

# COMMAND ----------

 final_race_df.write.mode("overwrite").format('parquet').partitionBy('race_year').saveAsTable('f1_processsed.races')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processsed.races

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SET timezone = India;
# MAGIC SELECT current_timezone();
# MAGIC Select current_timestamp();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  fi_analyse_prod.races