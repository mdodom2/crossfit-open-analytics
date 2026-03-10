{#
  Model: silver_open_leaderboard.sql
  Description:
    Silver model that flattens raw leaderboardRows into one typed row per athlete per
    competition slice, with standardized business keys and audit columns.

  Owner: Matthew Odom
  Materialization: view
  Refresh Cadence: On demand
  Grain: one row per athlete per year/division/scaled/region

  Dependencies:
    - {{ ref('bronze_crossfit__open_leaderboard_raw') }}

  Notes/Assumptions:
    - leaderboardRows is the primary athlete-level array in the payload.
    - Business keys are defined here and must remain consistent for Gold dimensions/facts.

  Create Date: 2026-03-09
  Last Modified: 2026-03-09
#}

{{
    config(
        materialized='view',
        alias='open_leaderboard'
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
        value as leaderboard_row
    FROM bronze_data,
    LATERAL FLATTEN(input => payload:leaderboardRows)

),

cleaned_data AS (

    SELECT
        -- business keys
        CONCAT(
            year::VARCHAR, '|',
            division::VARCHAR, '|',
            scaled::VARCHAR, '|',
            region::VARCHAR
        )::VARCHAR as competition_key,

        leaderboard_row:entrant:competitorId::VARCHAR as athlete_key,

        NULLIF(leaderboard_row:entrant:affiliateId::VARCHAR, 'None') as affiliate_key,
        leaderboard_row:entrant:regionId::VARCHAR as region_key,
        leaderboard_row:entrant:countryOfOriginCode::VARCHAR as country_key,
        leaderboard_row:entrant:divisionId::VARCHAR as division_key,

        -- competition context
        year::NUMBER(38,0) as competition_year,
        division::NUMBER(38,0) as competition_division,
        scaled::NUMBER(38,0) as competition_scaled,
        region::NUMBER(38,0) as competition_region,
        page::NUMBER(38,0) as source_page,

        -- athlete attributes
        leaderboard_row:entrant:competitorId::VARCHAR as competitor_id,
        TRIM(leaderboard_row:entrant:competitorName::VARCHAR) as competitor_name,
        TRIM(leaderboard_row:entrant:firstName::VARCHAR) as first_name,
        TRIM(leaderboard_row:entrant:lastName::VARCHAR) as last_name,
        UPPER(TRIM(leaderboard_row:entrant:gender::VARCHAR)) as gender,
        leaderboard_row:entrant:age::NUMBER(38,0) as age,
        TRIM(leaderboard_row:entrant:height::VARCHAR) as height,
        TRIM(leaderboard_row:entrant:weight::VARCHAR) as weight,
        NULLIF(TRIM(leaderboard_row:entrant:status::VARCHAR), '') as status,
        leaderboard_row:entrant:teamCaptain::VARCHAR as team_captain,
        NULLIF(TRIM(leaderboard_row:entrant:profilePicS3key::VARCHAR), '') as profile_pic_s3_key,

        -- affiliate / geography
        NULLIF(leaderboard_row:entrant:affiliateId::VARCHAR, 'None') as affiliate_id,
        NULLIF(TRIM(leaderboard_row:entrant:affiliateName::VARCHAR), '') as affiliate_name,
        leaderboard_row:entrant:regionId::VARCHAR as region_id,
        TRIM(leaderboard_row:entrant:regionName::VARCHAR) as region_name,
        leaderboard_row:entrant:countryOfOriginCode::VARCHAR as country_code,
        TRIM(leaderboard_row:entrant:countryOfOriginName::VARCHAR) as country_name,
        leaderboard_row:entrant:divisionId::VARCHAR as entrant_division_id,

        -- leaderboard measures
        leaderboard_row:overallRank::NUMBER(38,0) as overall_rank,
        leaderboard_row:overallScore::NUMBER(38,0) as overall_score,
        NULLIF(TRIM(leaderboard_row:nextStage::VARCHAR), '') as next_stage,

        -- useful flags / raw nested carry-forward
        leaderboard_row:ui:countryChampion::BOOLEAN as is_country_champion,
        leaderboard_row:ui:highlight::BOOLEAN as is_highlighted,
        leaderboard_row:scores as scores_array,

        -- audit columns
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS

    FROM flattened_rows
    WHERE leaderboard_row:entrant:competitorId IS NOT NULL
)

SELECT *
FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY competition_key, athlete_key
    ORDER BY SRC_UPD_TS DESC, source_page ASC
) = 1