# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

result_1=dbutils.notebook.run('1_circuits_csv',0)
print(result_1)

# COMMAND ----------

result_2=dbutils.notebook.run('2.races_csv_ingestion',0)
print(result_2)

# COMMAND ----------

result_3=dbutils.notebook.run('3.Constructor_json',0)
print(result_3)

# COMMAND ----------

result_4=dbutils.notebook.run('4.driver_json',0)
print(result_4)

# COMMAND ----------

result_5=dbutils.notebook.run('5.results_JSON',0)
print(result_5)

# COMMAND ----------

result_6=dbutils.notebook.run('6.pitstops_JSON',0)
print(result_6)

# COMMAND ----------

result_7=dbutils.notebook.run('7.lap_time_multiple_CSV',0)
print(result_7)

# COMMAND ----------

result_8=dbutils.notebook.run('8.qualification_multiple_multi-JSON',0)
print(result_8)