-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Finding Dominant Driver

-- COMMAND ----------

SELECT 
  driver_name,
  sum(calculated_points) as total_points,
  count(1) as total_races,
  avg(calculated_points) as avg_points
FROM f1_present_prod.calculated_race_results
WHERE race_year BETWEEN 1900 AND 2000
GROUP BY driver_name
HAVING total_races >=50
ORDER BY avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Finding Dominant Team

-- COMMAND ----------

SELECT
  team_name,
  sum(calculated_points) AS total_points,
  count(1) AS no_of_races,
  avg(calculated_points) AS avg_points
FROM f1_present_prod.calculated_race_results
GROUP BY team_name
HAVING no_of_races >=50
ORDER by avg_points DESC