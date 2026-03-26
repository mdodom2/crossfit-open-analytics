{#
  Model: gold_dim_athlete.sql
  Description:
    Gold dimension for CrossFit Open athletes.

  Grain:
    one row per athlete
#}

{{
    config(
        materialized='table',
        alias='dim_athlete'
    )
}}

WITH base AS (

    SELECT *
    FROM {{ ref('silver_athlete') }}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['athlete_key']) }} AS athlete_sk,
        athlete_key,
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
        athlete_status,
        team_captain,
        profile_pic_s3_key,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS
    FROM base

)

SELECT *
FROM final