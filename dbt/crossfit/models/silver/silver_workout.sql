{#
  Model: silver_workout.sql
  Description:
    Silver entity model representing workouts within a competition slice.

  Grain:
    one row per competition_key + workout_number
#}

{{
    config(
        materialized='view',
        alias='silver_workout'
    )
}}

WITH bronze_data AS (

    SELECT
        year,
        division,
        scaled,
        region,
        payload,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('bronze_crossfit__open_leaderboard_raw') }}

),

flattened_rows AS (

    SELECT
        year,
        division,
        scaled,
        region,
        value AS leaderboard_row,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM bronze_data,
    LATERAL FLATTEN(input => payload:leaderboardRows)

),

flattened_scores AS (

    SELECT
        CONCAT(
            year::VARCHAR, '|',
            division::VARCHAR, '|',
            scaled::VARCHAR, '|',
            region::VARCHAR
        )::VARCHAR AS competition_key,
        CONCAT(
            year::VARCHAR, '|',
            division::VARCHAR, '|',
            scaled::VARCHAR, '|',
            region::VARCHAR, '|',
            score.value:ordinal::VARCHAR
        )::VARCHAR AS workout_key,
        year::NUMBER(38,0) AS competition_year,
        division::NUMBER(38,0) AS competition_division,
        scaled::NUMBER(38,0) AS competition_scaled,
        region::NUMBER(38,0) AS competition_region,
        TRY_TO_NUMBER(score.value:ordinal::STRING) AS workout_number,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM flattened_rows,
    LATERAL FLATTEN(input => leaderboard_row:scores) score
    WHERE score.value:ordinal IS NOT NULL

),

deduped AS (

    SELECT *
    FROM flattened_scores
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workout_key
        ORDER BY SRC_UPD_TS DESC, SRC_CRT_TS DESC
    ) = 1

)

SELECT *
FROM deduped