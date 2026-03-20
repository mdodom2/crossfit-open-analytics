{#
  Model: gold_fct_open_leaderboard.sql
  Description:
    Derived leaderboard fact representing rolled-up competition results
    for each athlete within a competition slice. This model is sourced
    from the atomic workout event fact.

  Owner: Matthew Odom
  Materialization: table
  Grain: one row per athlete per competition slice

  Dependencies:
    - {{ ref('gold_fct_open_workout_scores') }}

  Create Date: 2026-03-10
#}

{{
    config(
        materialized='table',
        alias='gold_agg_open_leaderboard',
        tags=['fact','leaderboard']
    )
}}

-- Derived aggregate built from the atomic workout event fact.
-- One row per athlete per competition slice.

WITH source_data AS (

    SELECT
        athlete_sk,
        competition_sk,
        overall_rank,
        overall_score,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('gold_fct_open_workout_scores') }}

),

deduped AS (

    SELECT
        athlete_sk,
        competition_sk,
        overall_rank,
        overall_score,
        MIN(SRC_CRT_TS) AS SRC_CRT_TS,
        MAX(SRC_UPD_TS) AS SRC_UPD_TS,
        MAX(SRC_SYS) AS SRC_SYS
    FROM source_data
    GROUP BY
        athlete_sk,
        competition_sk,
        overall_rank,
        overall_score

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['athlete_sk','competition_sk']) }} AS leaderboard_sk,
        athlete_sk,
        competition_sk,
        overall_rank,
        overall_score,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM deduped

)

SELECT *
FROM final