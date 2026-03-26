{#
  Model: silver_region.sql
  Description:
    Silver entity model representing unique CrossFit Open regions.

  Grain:
    one row per region
#}

{{
    config(
        materialized='view',
        alias='silver_region'
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
        leaderboard_row:entrant:regionId::VARCHAR AS region_key,
        leaderboard_row:entrant:regionId::VARCHAR AS region_id,
        TRIM(leaderboard_row:entrant:regionName::VARCHAR) AS region_name,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM flattened_rows
    WHERE leaderboard_row:entrant:regionId IS NOT NULL

),

deduped AS (

    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY region_key
        ORDER BY SRC_UPD_TS DESC, SRC_CRT_TS DESC
    ) = 1

)

SELECT *
FROM deduped