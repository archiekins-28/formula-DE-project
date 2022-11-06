# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn('ingestion_date',current_timestamp())
    return output_df

# COMMAND ----------

#rearange columns
col=[]
def rearange_col(input_df,partition_col):
    for col_names in input_df.schema.names:
        if col_names != partition_col:
            col.append(col_names)
    col.append(partition_col)
    return input_df.select(col)

# COMMAND ----------

#Incremental Load Function
def incremental (input_df,db_name,tbl_name,partition_col):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    output_df=rearange_col(input_df,partition_col)
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{tbl_name}"):
        output_df.write.mode('overwrite').insertInto(f'{db_name}.{tbl_name}')
    else:
        output_df.write.mode('overwrite').format('parquet').partitionBy(f'{partition_col}').saveAsTable(f'{db_name}.{tbl_name}')  
    