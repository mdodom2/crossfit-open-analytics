{#
  Model: silver_date.sql
  Description:
    Silver entity model representing competition-year date concepts.

  Owner: Matthew Odom
  Materialization: view
  Grain: one row per year

  Dependencies:
    - {{ ref('silver_open_leaderboard_base') }}
#}

{{
    config(
        materialized='view',
        alias='date'
    )
}}

WITH base AS (

    SELECT DISTINCT
        competition_year,
        TO_DATE(CONCAT(competition_year, '-01-01')) AS calendar_date,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('silver_open_leaderboard_base') }}
    WHERE competition_year IS NOT NULL

)

SELECT *
FROM base