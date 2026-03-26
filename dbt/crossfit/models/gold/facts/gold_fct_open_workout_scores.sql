{#
  Model: gold_fct_open_workout_scores.sql
  Description:
    Gold fact table representing athlete performance at the workout level
    for the CrossFit Open.

  Grain:
    One row per athlete per workout per competition slice.

  Dependencies:
    - {{ ref('silver_open_workout_scores') }}

  Notes:
    - No dependency on gold dimensions for population
    - Surrogate keys derived from business keys
    - Designed for downstream joins to dimensions
#}

{{
    config(
        materialized='table',
        alias='fct_open_workout_scores'
    )
}}

WITH base AS (

    SELECT *
    FROM {{ ref('silver_open_workout_scores') }}

),

final AS (

    SELECT

        -- ========================
        -- SURROGATE KEYS (derived)
        -- ========================
        {{ dbt_utils.generate_surrogate_key(['athlete_key']) }}        AS athlete_sk,
        {{ dbt_utils.generate_surrogate_key(['competition_key']) }}    AS competition_sk,
        {{ dbt_utils.generate_surrogate_key(['workout_key']) }}        AS workout_sk,
        {{ dbt_utils.generate_surrogate_key(['athlete_workout_key']) }} AS athlete_workout_sk,

        -- ========================
        -- BUSINESS KEYS
        -- ========================
        athlete_key,
        competition_key,
        workout_key,
        athlete_workout_key,
        affiliate_key,
        region_key,
        country_key,
        division_key,

        -- ========================
        -- COMPETITION CONTEXT
        -- ========================
        competition_year,
        competition_division,
        competition_scaled,
        competition_region,

        -- ========================
        -- ATHLETE ATTRIBUTES (denormalized for performance)
        -- ========================
        competitor_id,
        competitor_name,
        first_name,
        last_name,
        gender,
        age,
        height,
        weight,

        affiliate_id,
        affiliate_name,
        region_id,
        region_name,
        country_code,
        country_name,
        entrant_division_id,

        -- ========================
        -- OVERALL PERFORMANCE
        -- ========================
        overall_rank,
        overall_score,
        next_stage,

        -- ========================
        -- WORKOUT PERFORMANCE
        -- ========================
        workout_number,
        workout_rank,
        workout_score_raw,
        workout_score_display,
        time_seconds,
        score_identifier,

        -- ========================
        -- VALIDATION / JUDGING
        -- ========================
        judge_name,
        judge_user_id,
        heat,
        lane,
        workout_affiliate_name,

        -- ========================
        -- FLAGS
        -- ========================
        is_valid,
        has_video,
        is_scaled_workout,

        -- ========================
        -- DESCRIPTIVE
        -- ========================
        workout_breakdown,
        mobile_score_display,

        source_page,

        -- ========================
        -- AUDIT
        -- ========================
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CRT_TS,
        UPD_TS

    FROM base

)

SELECT *
FROM final