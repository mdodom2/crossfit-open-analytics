{{
    config(
        materialized='table',
        alias='gold_fct_open_workout_scores',
        tags=['fact','workout']
    )
}}

WITH base AS (

    SELECT
        athlete_key,
        competition_key,
        workout_key,
        overall_rank,
        overall_score,
        workout_rank,
        workout_score_raw,
        workout_score_display,
        is_valid,
        has_video,
        judge_user_id,
        affiliate_name,
        SRC_CRT_TS,
        SRC_UPD_TS,
        SRC_SYS
    FROM {{ ref('silver_open_workout_scores') }}

),

joined AS (

    SELECT
        a.athlete_sk,
        c.competition_sk,
        w.workout_sk,

        b.overall_rank,
        b.overall_score,
        b.workout_rank,
        b.workout_score_raw,
        b.workout_score_display,
        b.is_valid,
        b.has_video,
        b.judge_user_id,
        b.affiliate_name,

        b.SRC_CRT_TS,
        b.SRC_UPD_TS,
        b.SRC_SYS

    FROM base b

    LEFT JOIN {{ ref('gold_dim_athlete') }} a
        ON b.athlete_key = a.athlete_key

    LEFT JOIN {{ ref('gold_dim_competition') }} c
        ON b.competition_key = c.competition_key

    LEFT JOIN {{ ref('gold_dim_workout') }} w
        ON b.workout_key = w.workout_key

)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['athlete_sk','competition_sk','workout_sk']
    ) }} AS workout_score_sk,

    athlete_sk,
    competition_sk,
    workout_sk,

    overall_rank,
    overall_score,
    workout_rank,
    workout_score_raw,
    workout_score_display,

    is_valid,
    has_video,
    judge_user_id,
    affiliate_name,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS CRT_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPD_TS,

    SRC_CRT_TS,
    SRC_UPD_TS,
    SRC_SYS

FROM joined
WHERE athlete_sk IS NOT NULL
AND competition_sk IS NOT NULL
AND workout_sk IS NOT NULL