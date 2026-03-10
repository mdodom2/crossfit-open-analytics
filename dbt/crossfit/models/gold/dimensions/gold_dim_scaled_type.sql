{{
    config(
        materialized='table',
        alias='gold_dim_scaled_type',
        tags=['dimension','scaled']
    )
}}

WITH cleaned AS (

    SELECT DISTINCT
        competition_scaled as scaled_id
    FROM {{ ref('silver_open_leaderboard') }}
    WHERE competition_scaled IS NOT NULL

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['scaled_id']) }} as scaled_type_sk,
    scaled_id,

    CASE scaled_id
        WHEN 0 THEN 'Rx'
        WHEN 1 THEN 'Scaled'
        ELSE CONCAT('Scaled Type ', scaled_id)
    END as scaled_type_name,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_UPD_TS,
    'crossfit_lookup' as SRC_SYS
FROM cleaned