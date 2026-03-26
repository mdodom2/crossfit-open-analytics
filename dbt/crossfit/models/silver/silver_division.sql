{#
  Model: silver_division.sql
  Description:
    Silver entity model representing unique CrossFit Open divisions.

  Grain:
    one row per division
#}

{{
    config(
        materialized='view',
        alias='silver_division'
    )
}}

WITH bronze_data AS (

    SELECT
        division,
        payload,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('bronze_crossfit__open_leaderboard_raw') }}

),

division_from_context AS (

    SELECT
        TRY_TO_NUMBER(division) AS division_id,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM bronze_data
    WHERE division IS NOT NULL

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

division_from_entrant AS (

    SELECT
        TRY_TO_NUMBER(leaderboard_row:entrant:divisionId::STRING) AS division_id,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM flattened_rows
    WHERE leaderboard_row:entrant:divisionId IS NOT NULL

),

unioned AS (

    SELECT * FROM division_from_context
    UNION ALL
    SELECT * FROM division_from_entrant

),

cleaned AS (

    SELECT *
    FROM unioned
    WHERE division_id IS NOT NULL

),

deduped AS (

    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY division_id
        ORDER BY SRC_UPD_TS DESC, SRC_CRT_TS DESC
    ) = 1

)

SELECT
    division_id,
    SRC_CRT_TS,
    SRC_UPD_TS,
    SRC_SYS
FROM deduped