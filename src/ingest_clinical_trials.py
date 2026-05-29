import asyncio
from typing import Any

import httpx
import polars as pl
from loguru import logger

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL    = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE   = 1000          # max allowed by the API
MAX_RETRIES = 3
TIMEOUT     = 60.0


# ── Schema helpers ─────────────────────────────────────────────────────────────
def _flatten_study(study: dict[str, Any]) -> dict[str, Any]:
    """
    Flatten the nested ClinicalTrials JSON study record into a single-level
    dict suitable for a Polars row.  Only the most analytically useful fields
    are extracted; extend as needed.
    """
    proto   = study.get("protocolSection", {})
    id_mod  = proto.get("identificationModule",  {})
    stat_mod= proto.get("statusModule",          {})
    desc_mod= proto.get("descriptionModule",     {})
    design  = proto.get("designModule",          {})
    elig    = proto.get("eligibilityModule",     {})
    contacts= proto.get("contactsLocationsModule",{})
    sponsor = proto.get("sponsorCollaboratorsModule", {})
    outcomes= proto.get("outcomesModule",        {})
    interv  = proto.get("armsInterventionsModule",{})
    cond    = proto.get("conditionsModule",       {})

    # Interventions: join names
    interventions = interv.get("interventions", [])
    intr_names    = "|".join(i.get("name", "") for i in interventions)
    intr_types    = "|".join(i.get("type", "") for i in interventions)
    intr_drugs    = "|".join(
        i.get("name", "") for i in interventions if i.get("type") == "DRUG"
    )

    # Conditions
    conditions = "|".join(cond.get("conditions", []))

    # Locations: just count + first country
    locations   = contacts.get("locations", [])
    loc_count   = len(locations)
    loc_country = locations[0].get("country", None) if locations else None

    # Primary outcomes
    primary_outcomes = outcomes.get("primaryOutcomes", [])
    primary_outcome  = primary_outcomes[0].get("measure", None) if primary_outcomes else None

    return {
        "nct_id":                id_mod.get("nctId"),
        "brief_title":           id_mod.get("briefTitle"),
        "official_title":        id_mod.get("officialTitle"),
        "org_study_id":          id_mod.get("orgStudyIdInfo", {}).get("id"),
        "overall_status":        stat_mod.get("overallStatus"),
        "start_date":            stat_mod.get("startDateStruct", {}).get("date"),
        "primary_completion_date": stat_mod.get("primaryCompletionDateStruct", {}).get("date"),
        "completion_date":       stat_mod.get("completionDateStruct", {}).get("date"),
        "study_first_posted":    stat_mod.get("studyFirstPostDateStruct", {}).get("date"),
        "last_update_posted":    stat_mod.get("lastUpdatePostDateStruct", {}).get("date"),
        "brief_summary":         desc_mod.get("briefSummary"),
        "study_type":            design.get("studyType"),
        "phases":                "|".join(design.get("phases", [])),
        "enrollment":            design.get("enrollmentInfo", {}).get("count"),
        "enrollment_type":       design.get("enrollmentInfo", {}).get("type"),
        "allocation":            design.get("designInfo", {}).get("allocation"),
        "intervention_model":    design.get("designInfo", {}).get("interventionModel"),
        "primary_purpose":       design.get("designInfo", {}).get("primaryPurpose"),
        "masking":               design.get("designInfo", {}).get("maskingInfo", {}).get("masking"),
        "eligibility_criteria":  elig.get("eligibilityCriteria"),
        "healthy_volunteers":    elig.get("healthyVolunteers"),
        "sex":                   elig.get("sex"),
        "minimum_age":           elig.get("minimumAge"),
        "maximum_age":           elig.get("maximumAge"),
        "std_ages":              "|".join(elig.get("stdAges", [])),
        "lead_sponsor_name":     sponsor.get("leadSponsor", {}).get("name"),
        "lead_sponsor_class":    sponsor.get("leadSponsor", {}).get("class"),
        "conditions":            conditions,
        "intervention_names":    intr_names,
        "intervention_types":    intr_types,
        "drug_interventions":    intr_drugs,
        "location_count":        loc_count,
        "first_location_country":loc_country,
        "primary_outcome":       primary_outcome,
    }


# ── Page fetcher ───────────────────────────────────────────────────────────────
async def _fetch_page(
    client:     httpx.AsyncClient,
    page_token: str | None,
    params:     dict[str, Any],
) -> tuple[list[dict], str | None]:
    """
    Fetch a single page of studies. Returns (records, next_page_token).
    next_page_token is None when there are no more pages.
    """
    request_params = {**params, "pageSize": PAGE_SIZE, "format": "json"}
    if page_token:
        request_params["pageToken"] = page_token

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.get(BASE_URL, params=request_params)
            response.raise_for_status()
            data       = response.json()
            studies    = data.get("studies", [])
            next_token = data.get("nextPageToken")
            return studies, next_token
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP {e.response.status_code} on attempt {attempt}: {e}")
        except Exception as e:
            logger.warning(f"Error on attempt {attempt}: {e}")

        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)   # exponential back-off

    logger.error("Max retries exceeded for a page — skipping.")
    return [], None


# ── Full ingest ────────────────────────────────────────────────────────────────
async def get_full_clinical_trials_async(
    query_params: dict[str, Any] | None = None,
    concurrency:  int = 5,
) -> pl.DataFrame:
    """
    Fetch all studies from the ClinicalTrials.gov v2 API matching
    `query_params` and return a flat Polars DataFrame.

    query_params examples
    ─────────────────────
    {}                                          → all studies
    {"query.cond": "diabetes"}                  → condition filter
    {"query.intr": "ibuprofen"}                 → intervention filter
    {"filter.overallStatus": "COMPLETED"}       → status filter
    {"query.cond": "cancer", "filter.phase": "PHASE3"}  → combined

    See https://clinicaltrials.gov/data-api/api for full parameter list.
    """
    params = query_params or {}
    logger.info(f"Starting ClinicalTrials.gov ingest with params: {params}")

    all_records: list[dict] = []
    page_token: str | None  = None
    page_num                = 0

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0; +https://clinicaltrials.gov)"
    }
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers) as client:
        while True:
            page_num += 1
            logger.info(f"Fetching page {page_num} (collected {len(all_records)} so far)…")

            studies, next_token = await _fetch_page(client, page_token, params)

            for study in studies:
                try:
                    all_records.append(_flatten_study(study))
                except Exception as e:
                    nct = study.get("protocolSection", {}) \
                                .get("identificationModule", {}) \
                                .get("nctId", "UNKNOWN")
                    logger.warning(f"Failed to flatten study {nct}: {e}")

            if not next_token:
                logger.info(f"Ingest complete — {len(all_records)} total studies fetched.")
                break

            page_token = next_token

    if not all_records:
        return pl.DataFrame()

    return pl.DataFrame(all_records, infer_schema_length=len(all_records))
