{#
  Model: gold_dim_region.sql
  Description:
    Gold dimension for regions.

  Grain:
    one row per region
#}

{{
    config(
        materialized='table',
        alias='dim_region'
    )
}}

WITH base AS (

    SELECT *
    FROM {{ ref('silver_region') }}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['region_key']) }} AS region_sk,
        region_key,
        region_id,
        region_name,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS
    FROM base

)

SELECT *
FROM final