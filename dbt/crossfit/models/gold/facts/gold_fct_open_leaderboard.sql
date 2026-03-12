{#
  Model: gold_fct_open_leaderboard.sql
  Description:
    Fact table representing final leaderboard results for each athlete
    within a competition slice (year/division/scaled/region).

  Owner: Matthew Odom
  Materialization: table
  Grain: one row per athlete per competition slice

  Dependencies:
    - {{ ref('silver_open_leaderboard') }}
    - {{ ref('gold_dim_athlete') }}
    - {{ ref('gold_dim_competition') }}

  Create Date: 2026-03-10
#}

{{
    config(
        materialized='table',
        alias='gold_fct_open_leaderboard',
        tags=['fact','leaderboard']
    )
}}

WITH source_data AS (

    SELECT
        athlete_key,
        competition_key,
        overall_rank,
        overall_score,
        next_stage,
        is_country_champion,
        is_highlighted,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('silver_open_leaderboard') }}

),

keys AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['athlete_key']) }} as athlete_sk,
        {{ dbt_utils.generate_surrogate_key(['competition_key']) }} as competition_sk,

        athlete_key,
        competition_key,

        overall_rank,
        overall_score,
        next_stage,
        is_country_champion,
        is_highlighted,

        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS

    FROM source_data

)

SELECT

    {{ dbt_utils.generate_surrogate_key(['athlete_key','competition_key']) }} as leaderboard_sk,

    athlete_sk,
    competition_sk,

    overall_rank,
    overall_score,
    next_stage,
    is_country_champion,
    is_highlighted,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS,

    SRC_CRT_TS,
    SRC_UPD_TS,
    SRC_SYS

FROM keys