{#
  Model: gold_dim_competition.sql
  Description:
    Gold dimension for competition slices.

  Grain:
    one row per competition slice
#}

{{
    config(
        materialized='table',
        alias='dim_competition'
    )
}}

WITH base AS (

    SELECT *
    FROM {{ ref('silver_competition') }}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['competition_key']) }} AS competition_sk,
        competition_key,
        competition_year,
        competition_division,
        competition_scaled,
        competition_region,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS
    FROM base

)

SELECT *
FROM final