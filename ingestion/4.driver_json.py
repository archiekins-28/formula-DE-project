# Databricks notebook source
# MAGIC %md
# MAGIC #### Read the drivers json file using api

# COMMAND ----------

# MAGIC %run "../utilities/parameters_NB"

# COMMAND ----------

)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

# As it's a nested JSON object here we are declaring the name schema alone
# [code: string, dob: string, driverId: bigint, driverRef: string, name: struct<forename:string,surname:string>, nationality: string, number: string, url: string]
name_schema=StructType(fields=[ StructField("forename",StringType(),True),\
                                StructField("surname",StringType(),True)
    
])

# COMMAND ----------

driver_schema=StructType(fields=[StructField("driverId",IntegerType(),False),\
                                 StructField("driverRef",StringType(),True),\
                                 StructField("number",IntegerType(),True),\
                                 StructField("code",StringType(),True),\
                                 StructField("name",name_schema),\
                                 StructField("dob",DateType(),True),\
                                 StructField("nationality",StringType(),True),\
                                 StructField("url",StringType(),True)
    
])

# COMMAND ----------

driver_df=spark.read\
.schema(driver_schema)\
.json(f'{raw_path}/drivers.json')

# COMMAND ----------

display(driver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implementing the new ingestion_date col and name column
# MAGIC ##### name.forename
# MAGIC ##### name.surname

# COMMAND ----------

from pyspark.sql.functions import concat,col,current_timestamp,lit

# COMMAND ----------

additional_driver_df=driver_df.withColumn('name',concat(col('name.forename'),lit(' '),col('name.surname')))\
.withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Selecting required column and renaming them

# COMMAND ----------

final_driver_df=additional_driver_df.select(col('driverId').alias('driver_id'),col('driverRef').alias('driver_ref'),col('number'),col('code'),col('name'),col('dob'))
                                            
                                           

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Writhing the data into the storage

# COMMAND ----------

final_driver_df.write.mode('overwrite').parquet(f'{processed_path}/processed/drivers')

# COMMAND ----------

display(spark.read.parquet(f'{processed_path}/processed/drivers'))