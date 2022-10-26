-- Databricks notebook source
DROP TABLE fi_analyse_prod.circuits

-- COMMAND ----------

DROP DATABASE IF EXISTS fi_analyse_prod;
CREATE DATABASE IF NOT EXISTS fi_analyse_prod
LOCATION '/mnt/formuladl282/processed/'

-- COMMAND ----------

DESC  DATABASE EXTENDED f1_raw_prod;

-- COMMAND ----------

SHOW DATABASES


-- COMMAND ----------

DROP DATABASE IF EXISTS f1_present_prod;
CREATE DATABASE IF NOT EXISTS f1_present_prod
LOCATION '/mnt/formuladl282/presentation/SQL/'