{#
  Model: gold_dim_competition.sql
  Description:
    Competition dimension for CrossFit Open leaderboard slices.
    One row per competition_key.

  Owner: Matthew Odom
  Materialization: table
  Grain: one row per competition slice
#}

{{
    config(
        materialized='table',
        alias='gold_dim_competition',
        tags=['dimension', 'competition']
    )
}}

WITH base AS (

    SELECT
        competition_key,
        competition_year,
        competition_division,
        competition_scaled,
        competition_region,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('silver_open_leaderboard') }}

),

deduped AS (

    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY competition_key
        ORDER BY SRC_UPD_TS DESC
    ) = 1

)

SELECT

    {{ dbt_utils.generate_surrogate_key(['competition_key']) }} as competition_sk,

    competition_key,
    competition_year,
    competition_division,
    competition_scaled,
    competition_region,

    'Open' as competition_type,

    CASE competition_division
        WHEN 1 THEN 'Men'
        WHEN 2 THEN 'Women'
        WHEN 18 THEN 'Men 35-39'
        ELSE CONCAT('Division ', competition_division)
    END as division_name,

    CASE competition_scaled
        WHEN 0 THEN 'Rx'
        WHEN 1 THEN 'Scaled'
        ELSE 'Unknown'
    END as scaled_name,

    CASE competition_region
        WHEN 0 THEN 'Worldwide'
        ELSE CONCAT('Region ', competition_region)
    END as region_name,

    CONCAT(
        competition_year,
        ' Open - ',
        CASE competition_division
            WHEN 1 THEN 'Men'
            WHEN 2 THEN 'Women'
            WHEN 18 THEN 'Men 35-39'
            ELSE CONCAT('Division ', competition_division)
        END,
        ' - ',
        CASE competition_scaled
            WHEN 0 THEN 'Rx'
            WHEN 1 THEN 'Scaled'
        END
    ) as competition_name,

    SRC_CRT_TS,
    SRC_UPD_TS,
    SRC_SYS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS

FROM deduped