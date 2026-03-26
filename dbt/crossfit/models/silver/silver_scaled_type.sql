{#
  Model: silver_scaled_type.sql
  Description:
    Silver entity model representing unique CrossFit Open scaled types.

  Grain:
    one row per scaled type
#}

{{
    config(
        materialized='view',
        alias='silver_scaled_type'
    )
}}

WITH bronze_data AS (

    SELECT
        scaled,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('bronze_crossfit__open_leaderboard_raw') }}

),

base AS (

    SELECT
        CONCAT('scaled|', scaled::VARCHAR) AS scaled_type_key,
        TRY_TO_NUMBER(scaled) AS scaled_id,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM bronze_data
    WHERE scaled IS NOT NULL

),

deduped AS (

    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY scaled_type_key
        ORDER BY SRC_UPD_TS DESC, SRC_CRT_TS DESC
    ) = 1

)

SELECT *
FROM deduped