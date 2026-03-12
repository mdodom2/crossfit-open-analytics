{{
    config(
        materialized='table',
        alias='gold_dim_athlete',
        tags=['dimension','athlete']
    )
}}

WITH base AS (

    SELECT
        competitor_id,
        athlete_key,
        competitor_name,
        first_name,
        last_name,
        gender,
        age,
        height,
        weight,
        affiliate_id,
        affiliate_name,
        region_id,
        region_name,
        country_code,
        country_name,
        entrant_division_id,
        status as athlete_status,
        team_captain,
        profile_pic_s3_key,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('silver_open_leaderboard') }}
    WHERE competitor_id IS NOT NULL

),

deduped AS (

    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY competitor_id
        ORDER BY SRC_UPD_TS DESC, SRC_CRT_TS DESC
    ) = 1

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['competitor_id']) }} as athlete_sk,

    competitor_id as athlete_key,
    competitor_id,
    competitor_name,
    first_name,
    last_name,
    gender,
    age,
    height,
    weight,
    affiliate_id,
    affiliate_name,
    region_id,
    region_name,
    country_code,
    country_name,
    entrant_division_id,
    athlete_status,
    team_captain,
    profile_pic_s3_key,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as UPD_TS,
    SRC_CRT_TS,
    SRC_UPD_TS,
    SRC_SYS

FROM deduped