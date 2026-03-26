{#
  Model: silver_open_workout_scores.sql
  Description:
    Silver model that explodes the nested leaderboardRows and scores arrays from the
    CrossFit Open raw payload into one typed row per athlete per workout per competition slice.

  Owner: Matthew Odom
  Materialization: view
  Refresh Cadence: On demand
  Grain: one row per athlete per workout per year/division/scaled/region

  Dependencies:
    - {{ ref('bronze_crossfit__open_leaderboard_raw') }}

  Notes/Assumptions:
    - workout ordinal from the payload is used as workout_number
    - business keys defined here must remain consistent for downstream gold dimensions/facts
    - this model intentionally sources directly from bronze to avoid silver-on-silver dependency

  Create Date: 2026-03-09
  Last Modified: 2026-03-25
#}

{{
    config(
        materialized='view',
        alias='open_workout_scores'
    )
}}

WITH bronze_data AS (

    SELECT
        year,
        division,
        scaled,
        region,
        page,
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
        page,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        value AS leaderboard_row
    FROM bronze_data,
    LATERAL FLATTEN(input => payload:leaderboardRows)

),

flattened_scores AS (

    SELECT
        -- competition business key
        CONCAT(
            year::VARCHAR, '|',
            division::VARCHAR, '|',
            scaled::VARCHAR, '|',
            region::VARCHAR
        )::VARCHAR AS competition_key,

        -- business keys from entrant / source payload
        leaderboard_row:entrant:competitorId::VARCHAR AS athlete_key,
        NULLIF(leaderboard_row:entrant:affiliateId::VARCHAR, 'None') AS affiliate_key,
        leaderboard_row:entrant:regionId::VARCHAR AS region_key,
        leaderboard_row:entrant:countryOfOriginCode::VARCHAR AS country_key,
        leaderboard_row:entrant:divisionId::VARCHAR AS division_key,

        -- competition context
        year::NUMBER(38,0) AS competition_year,
        division::NUMBER(38,0) AS competition_division,
        scaled::NUMBER(38,0) AS competition_scaled,
        region::NUMBER(38,0) AS competition_region,
        page::NUMBER(38,0) AS source_page,

        -- athlete attributes
        leaderboard_row:entrant:competitorId::VARCHAR AS competitor_id,
        TRIM(leaderboard_row:entrant:competitorName::VARCHAR) AS competitor_name,
        TRIM(leaderboard_row:entrant:firstName::VARCHAR) AS first_name,
        TRIM(leaderboard_row:entrant:lastName::VARCHAR) AS last_name,
        UPPER(TRIM(leaderboard_row:entrant:gender::VARCHAR)) AS gender,
        leaderboard_row:entrant:age::NUMBER(38,0) AS age,
        TRIM(leaderboard_row:entrant:height::VARCHAR) AS height,
        TRIM(leaderboard_row:entrant:weight::VARCHAR) AS weight,

        -- affiliate / geography
        NULLIF(leaderboard_row:entrant:affiliateId::VARCHAR, 'None') AS affiliate_id,
        NULLIF(TRIM(leaderboard_row:entrant:affiliateName::VARCHAR), '') AS affiliate_name,
        leaderboard_row:entrant:regionId::VARCHAR AS region_id,
        TRIM(leaderboard_row:entrant:regionName::VARCHAR) AS region_name,
        leaderboard_row:entrant:countryOfOriginCode::VARCHAR AS country_code,
        TRIM(leaderboard_row:entrant:countryOfOriginName::VARCHAR) AS country_name,
        leaderboard_row:entrant:divisionId::VARCHAR AS entrant_division_id,

        -- athlete leaderboard measures
        leaderboard_row:overallRank::NUMBER(38,0) AS overall_rank,
        leaderboard_row:overallScore::NUMBER(38,0) AS overall_score,
        NULLIF(TRIM(leaderboard_row:nextStage::VARCHAR), '') AS next_stage,

        -- nested score row
        score.value AS workout_score,

        -- audit columns
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS

    FROM flattened_rows,
    LATERAL FLATTEN(input => leaderboard_row:scores) score
    WHERE leaderboard_row:entrant:competitorId IS NOT NULL

),

deduped_scores AS (

    SELECT *
    FROM flattened_scores
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            competition_key,
            athlete_key,
            TRY_TO_NUMBER(workout_score:ordinal::STRING)
        ORDER BY
            SRC_UPD_TS DESC,
            source_page ASC
    ) = 1

),

final AS (

    SELECT
        -- business keys
        competition_key,
        athlete_key,

        CONCAT(
            competition_key, '|',
            workout_score:ordinal::VARCHAR
        )::VARCHAR AS workout_key,

        CONCAT(
            athlete_key, '|',
            competition_key, '|',
            workout_score:ordinal::VARCHAR
        )::VARCHAR AS athlete_workout_key,

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
        TRY_TO_NUMBER(workout_score:ordinal::STRING) AS workout_number,
        TRY_TO_NUMBER(workout_score:rank::STRING) AS workout_rank,
        workout_score:score::VARCHAR AS workout_score_raw,
        workout_score:scoreDisplay::VARCHAR AS workout_score_display,
        TRY_TO_NUMBER(workout_score:time::STRING) AS time_seconds,
        workout_score:scoreIdentifier::VARCHAR AS score_identifier,

        -- judging / validation
        NULLIF(TRIM(workout_score:judge::VARCHAR), '') AS judge_name,
        NULLIF(TRIM(workout_score:judge_user_id::VARCHAR), '') AS judge_user_id,
        NULLIF(TRIM(workout_score:heat::VARCHAR), '') AS heat,
        NULLIF(TRIM(workout_score:lane::VARCHAR), '') AS lane,
        NULLIF(TRIM(workout_score:affiliate::VARCHAR), '') AS workout_affiliate_name,

        -- flags
        TRY_TO_BOOLEAN(workout_score:valid::VARCHAR) AS is_valid,
        TRY_TO_BOOLEAN(workout_score:video::VARCHAR) AS has_video,
        TRY_TO_BOOLEAN(workout_score:scaled::VARCHAR) AS is_scaled_workout,

        -- descriptive fields
        NULLIF(TRIM(workout_score:breakdown::VARCHAR), '') AS workout_breakdown,
        NULLIF(TRIM(workout_score:mobileScoreDisplay::VARCHAR), '') AS mobile_score_display,

        source_page,

        -- audit columns
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS

    FROM deduped_scores
    WHERE workout_score IS NOT NULL
)

SELECT *
FROM final