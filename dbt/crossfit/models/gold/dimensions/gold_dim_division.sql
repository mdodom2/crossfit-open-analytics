{#
  Model: gold_dim_division.sql
  Description:
    Gold dimension for divisions.

  Grain:
    one row per division
#}

{{
    config(
        materialized='table',
        alias='dim_division'
    )
}}

WITH base AS (

    SELECT *
    FROM {{ ref('silver_division') }}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['division_id']) }} AS division_sk,
        division_id,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS
    FROM base

)

SELECT *
FROM final