{#
  Model: silver_open_workout_scores.sql
  Description:
    Silver model that explodes the nested scores array from the CrossFit Open
    leaderboard payload into one typed row per athlete per workout per competition slice.

  Owner: Matthew Odom
  Materialization: view
  Refresh Cadence: On demand
  Grain: one row per athlete per workout per year/division/scaled/region

  Dependencies:
    - {{ ref('silver_open_leaderboard') }}

  Notes/Assumptions:
    - workout ordinal from the payload is used as workout_number
    - business keys defined here must remain consistent for downstream gold dimensions/facts

  Create Date: 2026-03-09
  Last Modified: 2026-03-09
#}

{{
    config(
        materialized='view',
        alias='open_workout_scores'
    )
}}

WITH leaderboard_base AS (

    SELECT
        competition_key,
        athlete_key,
        affiliate_key,
        region_key,
        country_key,
        division_key,

        competition_year,
        competition_division,
        competition_scaled,
        competition_region,

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

        overall_rank,
        overall_score,
        next_stage,

        source_page,
        scores_array,

        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('silver_open_leaderboard') }}

),

flattened_scores AS (

    SELECT
        l.*,
        s.value as workout_score
    FROM leaderboard_base l,
    LATERAL FLATTEN(input => l.scores_array) s

),

final AS (

    SELECT
        -- business keys
        competition_key,
        athlete_key,

        CONCAT(
            competition_key, '|',
            workout_score:ordinal::VARCHAR
        )::VARCHAR as workout_key,

        CONCAT(
            athlete_key, '|',
            competition_key, '|',
            workout_score:ordinal::VARCHAR
        )::VARCHAR as athlete_workout_key,

        affiliate_key,
        region_key,
        country_key,
        division_key,

        -- competition context
        competition_year,
        competition_division,
        competition_scaled,
        competition_region,

        -- athlete context
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

        overall_rank,
        overall_score,
        next_stage,

        -- workout attributes
        TRY_TO_NUMBER(workout_score:ordinal::STRING) as workout_number,
        TRY_TO_NUMBER(workout_score:rank::STRING) as workout_rank,
        workout_score:score::VARCHAR as workout_score_raw,
        workout_score:scoreDisplay::VARCHAR as workout_score_display,
        TRY_TO_NUMBER(workout_score:time::STRING) as time_seconds,
        workout_score:scoreIdentifier::VARCHAR as score_identifier,

        -- judging / validation
        NULLIF(TRIM(workout_score:judge::VARCHAR), '') as judge_name,
        NULLIF(TRIM(workout_score:judge_user_id::VARCHAR), '') as judge_user_id,
        NULLIF(TRIM(workout_score:heat::VARCHAR), '') as heat,
        NULLIF(TRIM(workout_score:lane::VARCHAR), '') as lane,
        NULLIF(TRIM(workout_score:affiliate::VARCHAR), '') as workout_affiliate_name,

        -- flags
        TRY_TO_BOOLEAN(workout_score:valid::VARCHAR) as is_valid,
        TRY_TO_BOOLEAN(workout_score:video::VARCHAR) as has_video,
        TRY_TO_BOOLEAN(workout_score:scaled::VARCHAR) as is_scaled_workout,

        -- descriptive fields
        NULLIF(TRIM(workout_score:breakdown::VARCHAR), '') as workout_breakdown,
        NULLIF(TRIM(workout_score:mobileScoreDisplay::VARCHAR), '') as mobile_score_display,

        source_page,

        -- audit columns
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS

    FROM flattened_scores
    WHERE workout_score IS NOT NULL
)

SELECT *
FROM final