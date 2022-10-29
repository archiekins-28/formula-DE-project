-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style ="color:Black;text-align:center;font-family:Ariel"> Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

DROP VIEW IF EXISTS v_dominant_drivers;
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT 
  driver_name,
  sum(calculated_points) as total_points,
  count(1) as total_races,
  avg(calculated_points) as avg_points,
  rank() OVER(ORDER BY avg(calculated_points) DESC) driver_rank
FROM f1_present_prod.calculated_race_results
-- WHERE race_year BETWEEN 1900 AND 2000
GROUP BY driver_name
HAVING total_races >=50
ORDER BY avg_points desc

-- COMMAND ----------


SELECT 
  driver_name,race_year,
  sum(calculated_points) as total_points,
  count(1) as total_races,
  avg(calculated_points) as avg_points
FROM f1_present_prod.calculated_race_results
WHERE race_year BETWEEN 1900 AND 2000
GROUP BY driver_name,race_year
ORDER BY race_year,avg_points desc

-- COMMAND ----------

SELECT 
  driver_name,race_year,
  sum(calculated_points) as total_points,
  count(1) as total_races,
  avg(calculated_points) as avg_points
FROM f1_present_prod.calculated_race_results
WHERE driver_name in (SELECT driver_name from v_dominant_drivers WHERE driver_rank <=10  )
GROUP BY driver_name,race_year
ORDER BY race_year,avg_points desc

-- COMMAND ----------

SELECT 
  driver_name,race_year,
  sum(calculated_points) as total_points,
  count(1) as total_races,
  avg(calculated_points) as avg_points
FROM f1_present_prod.calculated_race_results
WHERE driver_name in (SELECT driver_name from v_dominant_drivers WHERE driver_rank <=10  )
GROUP BY driver_name,race_year
ORDER BY race_year,avg_points desc

-- COMMAND ----------

SELECT 
  driver_name,race_year,
  sum(calculated_points) as total_points,
  count(1) as total_races,
  avg(calculated_points) as avg_points
FROM f1_present_prod.calculated_race_results
WHERE driver_name in (SELECT driver_name from v_dominant_drivers WHERE driver_rank <=10  )
GROUP BY driver_name,race_year
ORDER BY race_year,avg_points desc