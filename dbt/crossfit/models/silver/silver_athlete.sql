{#
  Model: silver_athlete.sql
  Description:
    Silver entity model representing unique athletes.

  Grain:
    one row per athlete
#}

{{
    config(
        materialized='view',
        alias='silver_athlete'
    )
}}

WITH bronze_data AS (

    SELECT
        payload,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('bronze_crossfit__open_leaderboard_raw') }}

),

flattened_rows AS (

    SELECT
        value AS leaderboard_row,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM bronze_data,
    LATERAL FLATTEN(input => payload:leaderboardRows)

),

base AS (

    SELECT
        leaderboard_row:entrant:competitorId::VARCHAR AS athlete_key,
        leaderboard_row:entrant:competitorId::VARCHAR AS competitor_id,
        TRIM(leaderboard_row:entrant:competitorName::VARCHAR) AS competitor_name,
        TRIM(leaderboard_row:entrant:firstName::VARCHAR) AS first_name,
        TRIM(leaderboard_row:entrant:lastName::VARCHAR) AS last_name,
        UPPER(TRIM(leaderboard_row:entrant:gender::VARCHAR)) AS gender,
        leaderboard_row:entrant:age::NUMBER(38,0) AS age,
        TRIM(leaderboard_row:entrant:height::VARCHAR) AS height,
        TRIM(leaderboard_row:entrant:weight::VARCHAR) AS weight,
        NULLIF(leaderboard_row:entrant:affiliateId::VARCHAR, 'None') AS affiliate_id,
        NULLIF(TRIM(leaderboard_row:entrant:affiliateName::VARCHAR), '') AS affiliate_name,
        leaderboard_row:entrant:regionId::VARCHAR AS region_id,
        TRIM(leaderboard_row:entrant:regionName::VARCHAR) AS region_name,
        leaderboard_row:entrant:countryOfOriginCode::VARCHAR AS country_code,
        TRIM(leaderboard_row:entrant:countryOfOriginName::VARCHAR) AS country_name,
        leaderboard_row:entrant:divisionId::VARCHAR AS entrant_division_id,
        NULLIF(TRIM(leaderboard_row:entrant:status::VARCHAR), '') AS athlete_status,
        leaderboard_row:entrant:teamCaptain::VARCHAR AS team_captain,
        NULLIF(TRIM(leaderboard_row:entrant:profilePicS3key::VARCHAR), '') AS profile_pic_s3_key,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM flattened_rows
    WHERE leaderboard_row:entrant:competitorId IS NOT NULL

),

deduped AS (

    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY athlete_key
        ORDER BY SRC_UPD_TS DESC, SRC_CRT_TS DESC
    ) = 1

)

SELECT *
FROM deduped