SELECT
-- Totals and Demographics (Converted to BigInt for calculations)
    Total_Population,
    Male_Population,
    Female_Population,

    -- Age Groups (Converted to BigInt)
    Pop_Under_5,
    Pop_5_To_9,
    Pop_10_To_14,
    Pop_15_To_19,
    Pop_20_To_24,
    Pop_25_To_34,
    Pop_35_To_44,
    Pop_45_To_54,
    Pop_55_To_59,
    Pop_60_To_64,
    Pop_65_To_74,
    Pop_75_To_84,
    Pop_85_Plus,

    -- BROADER AGE CATEGORIES (Aggregated)
    Pop_0_To_19_Approx,
    Pop_20_To_54_Approx,
    Pop_50_To_64_Approx,
    Pop_65_Plus,

    -- Race and Ethnicity (Converted to BigInt)
    Pop_White,
    Pop_Black,
    Pop_Amer_Indian_AK_Native,
    Pop_Asian,
    Pop_Hawaiian_Pac_Islander,
    Pop_Other_Race,

    -- Geography (Kept as Strings/Ints depending on use)
    State_Code,
    County_Code,
    FIPS_Code

FROM {{ref('stg_census')}} AS census