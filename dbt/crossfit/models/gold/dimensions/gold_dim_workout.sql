{#
  Model: gold_dim_workout.sql
  Description:
    Gold dimension for workouts within a competition slice.

  Grain:
    one row per competition_key + workout_number
#}

{{
    config(
        materialized='table',
        alias='dim_workout'
    )
}}

WITH base AS (

    SELECT *
    FROM {{ ref('silver_workout') }}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['workout_key']) }} AS workout_sk,
        workout_key,
        competition_key,
        competition_year,
        competition_division,
        competition_scaled,
        competition_region,
        workout_number,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS
    FROM base

)

SELECT *
FROM final