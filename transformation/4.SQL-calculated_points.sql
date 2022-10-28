-- Databricks notebook source
use fi_analyse_prod

-- COMMAND ----------

CREATE TABLE f1_present_prod.calculated_race_results
USING parquet
AS
SELECT 
  races.race_year,
  drivers.name AS driver_name,
  constructor.name AS team_name,
  results.points,
  results.position,
  11-results.position AS calculated_points
FROM results 
JOIN drivers ON (results.driver_id=drivers.driver_id)
JOIN constructor ON (results.constructor_id=constructor.constructor_id)
JOIN races ON (results.race_id=races.race_id)
WHERE results.position <=10