SELECT
-- Totals and Demographics (Converted to BigInt for calculations)
    CAST(NULLIF(TRIM("Total_Population"), '') AS BIGINT) AS Total_Population,
    CAST(NULLIF(TRIM("Male_Population"), '') AS BIGINT) AS Male_Population,
    CAST(NULLIF(TRIM("Female_Population"), '') AS BIGINT) AS Female_Population,

    -- Age Groups (Converted to BigInt)
    CAST(NULLIF(TRIM("Under_5_Population"), '') AS BIGINT) AS Pop_Under_5,
    CAST(NULLIF(TRIM("5_To_9_Population"), '') AS BIGINT) AS Pop_5_To_9,
    CAST(NULLIF(TRIM("10_To_14_Population"), '') AS BIGINT) AS Pop_10_To_14,
    CAST(NULLIF(TRIM("15_To_19_Population"), '') AS BIGINT) AS Pop_15_To_19,
    CAST(NULLIF(TRIM("20_To_24_Population"), '') AS BIGINT) AS Pop_20_To_24,
    CAST(NULLIF(TRIM("25_To_34_Population"), '') AS BIGINT) AS Pop_25_To_34,
    CAST(NULLIF(TRIM("35_To_44_Population"), '') AS BIGINT) AS Pop_35_To_44,
    CAST(NULLIF(TRIM("45_To_54_Population"), '') AS BIGINT) AS Pop_45_To_54,
    CAST(NULLIF(TRIM("55_To_59_Population"), '') AS BIGINT) AS Pop_55_To_59,
    CAST(NULLIF(TRIM("60_To_64_Population"), '') AS BIGINT) AS Pop_60_To_64,
    CAST(NULLIF(TRIM("65_To_74_Population"), '') AS BIGINT) AS Pop_65_To_74,
    CAST(NULLIF(TRIM("75_To_84_Population"), '') AS BIGINT) AS Pop_75_To_84,
    CAST(NULLIF(TRIM("85_Plus_Population"), '') AS BIGINT) AS Pop_85_Plus,

    -- BROADER AGE CATEGORIES (Aggregated)
    
    -- "0 - 19 years"
    (CAST(NULLIF(TRIM("Under_5_Population"), '') AS BIGINT) + 
    CAST(NULLIF(TRIM("5_To_9_Population"), '') AS BIGINT) + 
    CAST(NULLIF(TRIM("10_To_14_Population"), '') AS BIGINT) +
    CAST(NULLIF(TRIM("15_To_19_Population"), '') AS BIGINT)) AS Pop_0_To_19_Approx,

    -- "20 to 49 years"
    (CAST(NULLIF(TRIM("20_To_24_Population"), '') AS BIGINT) + 
    CAST(NULLIF(TRIM("25_To_34_Population"), '') AS BIGINT) + 
    CAST(NULLIF(TRIM("35_To_44_Population"), '') AS BIGINT) +
    CAST(NULLIF(TRIM("45_To_54_Population"), '') AS BIGINT)) AS Pop_20_To_54_Approx,

    -- "50 to 64 years"
    (CAST(NULLIF(TRIM("55_To_59_Population"), '') AS BIGINT) + 
    CAST(NULLIF(TRIM("60_To_64_Population"), '') AS BIGINT)) AS Pop_50_To_64_Approx,

    -- "65+ years"
    (CAST(NULLIF(TRIM("65_To_74_Population"), '') AS BIGINT) + 
    CAST(NULLIF(TRIM("75_To_84_Population"), '') AS BIGINT) + 
    CAST(NULLIF(TRIM("85_Plus_Population"), '') AS BIGINT)) AS Pop_65_Plus,

    -- Race and Ethnicity (Converted to BigInt)
    CAST(NULLIF(TRIM("White_Population"), '') AS BIGINT) AS Pop_White,
    CAST(NULLIF(TRIM("Black_Or_African_American_Population"), '') AS BIGINT) AS Pop_Black,
    CAST(NULLIF(TRIM("American_Indian_And_Alaska_Native_Population"), '') AS BIGINT) AS Pop_Amer_Indian_AK_Native,
    CAST(NULLIF(TRIM("Asian_Population"), '') AS BIGINT) AS Pop_Asian,
    CAST(NULLIF(TRIM("Native_Hawaiian_And_Other_Pacific_Islander_Population"), '') AS BIGINT) AS Pop_Hawaiian_Pac_Islander,
    CAST(NULLIF(TRIM("Some_Other_Race_Population"), '') AS BIGINT) AS Pop_Other_Race,

    -- Geography (Kept as Strings/Ints depending on use)
    TRY_CAST(NULLIF(TRIM("state"), '') AS INT) AS State_Code,
    TRY_CAST(NULLIF(TRIM("county"), '') AS INT) AS County_Code,
    TRY_CAST(NULLIF(TRIM("fips_code"), '') AS INT) AS FIPS_Code

FROM {{ source('ADVERSE_DRUGS', 'CENSUS_FIPS') }}