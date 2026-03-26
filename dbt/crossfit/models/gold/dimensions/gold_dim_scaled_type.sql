{#
  Model: gold_dim_scaled_type.sql
  Description:
    Gold dimension for scaled type.

  Grain:
    one row per scaled type
#}

{{
    config(
        materialized='table',
        alias='dim_scaled_type'
    )
}}

WITH base AS (

    SELECT *
    FROM {{ ref('silver_scaled_type') }}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['scaled_type_key']) }} AS scaled_type_sk,
        scaled_type_key,
        scaled_id,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS
    FROM base

)

SELECT *
FROM final