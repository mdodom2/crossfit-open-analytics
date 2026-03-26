{#
  Model: silver_competition.sql
  Description:
    Silver entity model representing unique competition slices.

  Grain:
    one row per competition slice
#}

{{
    config(
        materialized='view',
        alias='silver_competition'
    )
}}

WITH bronze_data AS (

    SELECT
        year,
        division,
        scaled,
        region,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('bronze_crossfit__open_leaderboard_raw') }}

),

base AS (

    SELECT
        CONCAT(
            year::VARCHAR, '|',
            division::VARCHAR, '|',
            scaled::VARCHAR, '|',
            region::VARCHAR
        )::VARCHAR AS competition_key,
        year::NUMBER(38,0) AS competition_year,
        TRY_TO_NUMBER(division) AS competition_division,
        TRY_TO_NUMBER(scaled) AS competition_scaled,
        TRY_TO_NUMBER(region) AS competition_region,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM bronze_data
    WHERE year IS NOT NULL

),

deduped AS (

    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY competition_key
        ORDER BY SRC_UPD_TS DESC, SRC_CRT_TS DESC
    ) = 1

)

SELECT *
FROM deduped