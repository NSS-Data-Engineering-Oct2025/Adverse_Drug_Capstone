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
    TRY_CAST(NULLIF(TRIM("fips_code"), '') AS INT) AS FIPS_Code,

    -- State Abbreviation Mapping (Using FIPS codes evaluated as Integers)
    CASE TRY_CAST(NULLIF(TRIM("state"), '') AS INT)
        WHEN 1 THEN 'AL' WHEN 2 THEN 'AK' WHEN 4 THEN 'AZ' WHEN 5 THEN 'AR' WHEN 6 THEN 'CA'
        WHEN 8 THEN 'CO' WHEN 9 THEN 'CT' WHEN 10 THEN 'DE' WHEN 11 THEN 'DC' WHEN 12 THEN 'FL'
        WHEN 13 THEN 'GA' WHEN 15 THEN 'HI' WHEN 16 THEN 'ID' WHEN 17 THEN 'IL' WHEN 18 THEN 'IN'
        WHEN 19 THEN 'IA' WHEN 20 THEN 'KS' WHEN 21 THEN 'KY' WHEN 22 THEN 'LA' WHEN 23 THEN 'ME'
        WHEN 24 THEN 'MD' WHEN 25 THEN 'MA' WHEN 26 THEN 'MI' WHEN 27 THEN 'MN' WHEN 28 THEN 'MS'
        WHEN 29 THEN 'MO' WHEN 30 THEN 'MT' WHEN 31 THEN 'NE' WHEN 32 THEN 'NV' WHEN 33 THEN 'NH'
        WHEN 34 THEN 'NJ' WHEN 35 THEN 'NM' WHEN 36 THEN 'NY' WHEN 37 THEN 'NC' WHEN 38 THEN 'ND'
        WHEN 39 THEN 'OH' WHEN 40 THEN 'OK' WHEN 41 THEN 'OR' WHEN 42 THEN 'PA' WHEN 44 THEN 'RI'
        WHEN 45 THEN 'SC' WHEN 46 THEN 'SD' WHEN 47 THEN 'TN' WHEN 48 THEN 'TX' WHEN 49 THEN 'UT'
        WHEN 50 THEN 'VT' WHEN 51 THEN 'VA' WHEN 53 THEN 'WA' WHEN 54 THEN 'WV' WHEN 55 THEN 'WI'
        WHEN 56 THEN 'WY' WHEN 72 THEN 'PR'
        ELSE NULL
    END AS State_Abbreviation

FROM {{ source('ADVERSE_DRUGS', 'CENSUS_FIPS') }}