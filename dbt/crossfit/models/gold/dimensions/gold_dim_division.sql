{{
    config(
        materialized='table',
        alias='gold_dim_division',
        tags=['dimension','division']
    )
}}

WITH source_values AS (

    SELECT DISTINCT
        competition_division as division_id
    FROM {{ ref('silver_open_leaderboard') }}

    UNION

    SELECT DISTINCT
        TRY_TO_NUMBER(entrant_division_id) as division_id
    FROM {{ ref('silver_open_leaderboard') }}
    WHERE entrant_division_id IS NOT NULL

),

cleaned AS (

    SELECT DISTINCT
        division_id
    FROM source_values
    WHERE division_id IS NOT NULL

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['division_id']) }} as division_sk,
    division_id,

    CASE division_id
        WHEN 1 THEN 'Men'
        WHEN 2 THEN 'Women'
        WHEN 18 THEN 'Men 35-39'
        ELSE CONCAT('Division ', division_id)
    END as division_name,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as SRC_UPD_TS,
    'crossfit_lookup' as SRC_SYS
FROM cleaned