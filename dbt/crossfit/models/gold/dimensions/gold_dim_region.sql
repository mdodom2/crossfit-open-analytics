{{
    config(
        materialized='table',
        alias='gold_dim_region',
        tags=['dimension','region']
    )
}}

WITH source_values AS (

    SELECT DISTINCT
        competition_region as region_id,
        CASE
            WHEN competition_region = 0 THEN 'Worldwide'
            ELSE NULL
        END as region_name
    FROM {{ ref('silver_open_leaderboard') }}

    UNION

    SELECT DISTINCT
        TRY_TO_NUMBER(region_id) as region_id,
        region_name
    FROM {{ ref('silver_open_leaderboard') }}
    WHERE region_id IS NOT NULL

),

deduped AS (

    SELECT
        region_id,
        region_name
    FROM source_values
    WHERE region_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY region_id
        ORDER BY CASE WHEN region_name IS NOT NULL THEN 1 ELSE 2 END
    ) = 1

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['region_id']) }} as region_sk,
    region_id,
    COALESCE(region_name, CONCAT('Region ', region_id)) as region_name,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_UPD_TS,
    'crossfit_lookup' as SRC_SYS

FROM deduped