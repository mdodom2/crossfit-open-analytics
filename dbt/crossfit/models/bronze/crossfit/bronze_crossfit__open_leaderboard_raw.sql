{#
  Model: bronze_crossfit__open_leaderboard_raw.sql
  Description:
    Bronze model for raw CrossFit Open leaderboard payloads. Preserves source structure
    with minimal transformation and standard audit columns.

  Owner: Matthew Odom
  Materialization: incremental
  Refresh Cadence: On demand
  Grain: one row per API page per year/division/scaled/region

  Dependencies:
    - {{ source('crossfit_raw', 'OPEN_LEADERBOARD_RAW') }}

  Notes/Assumptions:
    - Source table is populated externally by Python ingestion.
    - This model standardizes audit columns and naming for downstream dbt layers.

  Create Date: 2026-03-09
  Last Modified: 2026-03-09
#}

{{
    config(
        materialized='incremental',
        unique_key='raw_page_sk',
        incremental_strategy='merge',
        alias='crossfit__open_leaderboard_raw'
    )
}}

WITH source_data AS (

    SELECT
        run_id,
        year,
        division,
        scaled,
        region,
        page,
        payload,
        load_ts
    FROM {{ source('crossfit_raw', 'OPEN_LEADERBOARD_RAW') }}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'year',
            'division',
            'scaled',
            'region',
            'page'
        ]) }} as raw_page_sk,

        run_id::VARCHAR as run_id,
        year::NUMBER(38,0) as year,
        division::NUMBER(38,0) as division,
        scaled::NUMBER(38,0) as scaled,
        region::NUMBER(38,0) as region,
        page::NUMBER(38,0) as page,
        payload as payload,

        load_ts::TIMESTAMP_NTZ as SRC_CRT_TS,
        load_ts::TIMESTAMP_NTZ as SRC_UPD_TS,
        'crossfit_api'::VARCHAR as SRC_SYS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS

    FROM source_data
)

SELECT * FROM final

{% if is_incremental() %}
WHERE SRC_UPD_TS > (SELECT COALESCE(MAX(SRC_UPD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}