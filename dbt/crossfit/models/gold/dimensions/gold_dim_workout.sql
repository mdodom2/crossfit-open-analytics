{#
  Model: gold_dim_workout.sql
  Description:
    Workout dimension for CrossFit Open events.
    One row per workout within a competition slice.

  Owner: Matthew Odom
  Materialization: table
  Refresh Cadence: On demand
  Grain: one row per competition_key + workout_number

  Dependencies:
    - {{ ref('silver_open_workout_scores') }}

  Create Date: 2026-03-09
  Last Modified: 2026-03-10
#}

{{
    config(
        materialized='table',
        alias='gold_dim_workout',
        tags=['dimension', 'workout']
    )
}}

WITH base AS (

    SELECT
        competition_key,
        workout_number,
        competition_year,
        competition_division,
        competition_scaled,
        competition_region,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('silver_open_workout_scores') }}
    WHERE workout_number IS NOT NULL

),

deduped AS (

    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY competition_key, workout_number
        ORDER BY SRC_UPD_TS DESC
    ) = 1

),

final AS (

    SELECT
        CONCAT(competition_key, '|', workout_number) as workout_key,
        competition_key,
        workout_number,
        competition_year,
        competition_division,
        competition_scaled,
        competition_region,
        CONCAT('Open ', competition_year, ' - Workout ', workout_number) as workout_name,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM deduped

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['workout_key']) }} as workout_sk,
    workout_key,
    competition_key,
    workout_number,
    competition_year,
    competition_division,
    competition_scaled,
    competition_region,
    workout_name,
    SRC_CRT_TS,
    SRC_UPD_TS,
    SRC_SYS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS
FROM final