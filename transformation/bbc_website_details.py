# Databricks notebook source
# MAGIC %md
# MAGIC #### Reading the required data from the files to buit the report

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_path}/processed/races")\
.withColumnRenamed('race_timestamp','race_date')\
.withColumnRenamed('name','race_name')


# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_path}/processed/circuits")\
.withColumnRenamed('location','circuit_location')


# COMMAND ----------

results_df=spark.read.parquet(f"{processed_path}/processed/results")\
.withColumnRenamed('time','race_time')

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_path}/processed/drivers")\
.withColumnRenamed('name','driver_name')\
.withColumnRenamed('number','driver_number')\
.withColumnRenamed('nationality','driver_location')


# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_path}/processed/constructor")\
.withColumnRenamed('name','team')


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Joining the data frames

# COMMAND ----------

race_circuit_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,"inner")\
.select(races_df.race_id,races_df.race_name,races_df.race_year,races_df.race_date,circuits_df.circuit_location)



# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=results_df.join(race_circuit_df,results_df.race_id==race_circuit_df.race_id,"inner")\
.join(drivers_df,drivers_df.driver_id==results_df.driver_id,'inner')\
.join(constructors_df,constructors_df.constructor_id==results_df.constructor_id,'inner')\
.select(races_df.race_name,races_df.race_year,races_df.race_date,circuits_df.circuit_location,\
       drivers_df.driver_name,drivers_df.driver_number,drivers_df.driver_location,constructors_df.team,\
       results_df.grid,results_df.fastest_lap,results_df.race_time,results_df.points)\
.withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

filtered_df=final_df.filter('race_year==2020 and race_name=="Abu Dhabi Grand Prix"').orderBy(final_df.points.desc())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Writing the data into the storage

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_path}/race_results')

# COMMAND ----------

display(final_df)