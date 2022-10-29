-- Databricks notebook source

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT
  team_name,
  sum(calculated_points) AS total_points,
  count(1) AS no_of_races,
  avg(calculated_points) AS avg_points,
  rank() OVER (ORDER BY avg(calculated_points) DESC) team_rank
FROM f1_present_prod.calculated_race_results
GROUP BY team_name
HAVING no_of_races >=100
ORDER by avg_points DESC

-- COMMAND ----------

SELECT
  team_name,race_year,
  sum(calculated_points) AS total_points,
  count(1) AS no_of_races,
  avg(calculated_points) AS avg_points
FROM f1_present_prod.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_teams where team_rank <=5 )
GROUP BY team_name,race_year
ORDER by avg_points DESC

-- COMMAND ----------

SELECT
  team_name,race_year,
  sum(calculated_points) AS total_points,
  count(1) AS no_of_races,
  avg(calculated_points) AS avg_points
FROM f1_present_prod.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_teams where team_rank <=10 )
GROUP BY team_name,race_year
ORDER by avg_points DESC

-- COMMAND ----------

SELECT
  team_name,race_year,
  sum(calculated_points) AS total_points,
  count(1) AS no_of_races,
  avg(calculated_points) AS avg_points
FROM f1_present_prod.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_teams where team_rank <=5 )
GROUP BY team_name,race_year
ORDER by avg_points DESC