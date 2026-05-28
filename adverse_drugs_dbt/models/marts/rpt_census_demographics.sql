{{
    config(materialized='table')
}}

SELECT
    -- Gender totals
    SUM(Male_Population)            AS male_pop,
    SUM(Female_Population)          AS female_pop,
    SUM(Total_Population)           AS total_pop,

    -- Age groups
    SUM(Pop_Under_5)                AS pop_under_5,
    SUM(Pop_5_To_9)                 AS pop_5_to_9,
    SUM(Pop_10_To_14)               AS pop_10_to_14,
    SUM(Pop_15_To_19)               AS pop_15_to_19,
    SUM(Pop_20_To_24)               AS pop_20_to_24,
    SUM(Pop_25_To_34)               AS pop_25_to_34,
    SUM(Pop_35_To_44)               AS pop_35_to_44,
    SUM(Pop_45_To_54)               AS pop_45_to_54,
    SUM(Pop_55_To_59)               AS pop_55_to_59,
    SUM(Pop_60_To_64)               AS pop_60_to_64,
    SUM(Pop_65_To_74)               AS pop_65_to_74,
    SUM(Pop_75_To_84)               AS pop_75_to_84,
    SUM(Pop_85_Plus)                AS pop_85_plus,

    State_Abbreviation
FROM {{ ref('dim_fips_census') }}
GROUP BY State_Abbreviation
