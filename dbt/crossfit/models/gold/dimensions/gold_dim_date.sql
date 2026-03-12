{{
    config(
        materialized='table',
        alias='gold_dim_date'
    )
}}

WITH date_spine AS (

SELECT
    DATEADD(day, seq4(), '2023-01-01') AS calendar_date
FROM TABLE(GENERATOR(ROWCOUNT => 2000))

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['calendar_date']) }} as date_sk,
    calendar_date,
    YEAR(calendar_date) as year,
    MONTH(calendar_date) as month,
    DAY(calendar_date) as day,
    DAYNAME(calendar_date) as day_name,
    MONTHNAME(calendar_date) as month_name,
    CURRENT_TIMESTAMP() as CRT_TS,
    CURRENT_TIMESTAMP() as UPD_TS
FROM date_spine