"""
collect_gtr.py
==============
Collect UK circular economy research projects from the UKRI Gateway to Research
(GtR) API, screen for genuine CE relevance, and output project-level data
plus a hand-coding sample for validation.

Method: a two-stage protocol following systematic-review conventions (PRISMA
2020; Booth et al. 2016).

  Stage 1 - IDENTIFICATION
    Broad keyword search against the GtR projects API using terms grounded in
    Kirchherr, Reike & Hekkert (2017) and the Ellen MacArthur Foundation
    Circular Economy Glossary (2026).

  Stage 2 - SCREENING
    Each project is classified against a three-tier inclusion rule
    (Tier 1 defining / Tier 2 characteristic / Tier 3 supplementary terms with
    Boolean logic). Screening checks title + abstract + technical summary +
    potential impact.

Flow: all projects are collected and flattened first, deduplicated on
project_id, then screened. Only unique, kept projects are enriched (lead org,
participant orgs, PI, funding value, and - with --sectors - impact sectors from
outcome records), so we never spend API calls on duplicates or dropped projects.

Resuming: enrichment is the slow phase, so for long runs it is checkpointed.
The screened set is saved once, then enriched rows are written to a checkpoint
file every --checkpoint-every projects. If the run is interrupted, simply run
the same command again: it reloads the screened set and the checkpoint, skips
the projects already enriched, and carries on. Delete the checkpoint files (or
use --fresh) to start over.

Outputs (in ./data/):
  raw/        -> raw JSON for each search term (written incrementally, kept for
                 reproducibility)
  processed/  -> three CSVs:
    1. gtr_projects_<timestamp>.csv          (kept projects)
    2. gtr_all_with_decision_<timestamp>.csv (all projects + filter decision)
    3. gtr_validation_sample_<timestamp>.csv    (hand-coding sample, stratified)
  checkpoints/ -> resume state (screened set + enriched-so-far); safe to delete
                  once a run has finished cleanly.

Run examples:
    python collect_gtr.py --size 25 --max-pages 1            (quick test)
    python collect_gtr.py --size 25 --max-pages 1 --sectors  (test with sectors)
    python collect_gtr.py --sectors                          (full run + sectors)
    python collect_gtr.py --outcomes                         (full run + outcome hrefs)
    python collect_gtr.py --sectors --fresh                  (ignore any checkpoint)
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
import pandas as pd
import requests
import sqlite3
import hashlib

# ---------------------------------------------------------------------------
# DIRECTORIES
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
PROC_DIR = DATA_DIR / "processed" / "gtr"

CACHE_DIR = ROOT_DIR / "cache"
CKPT_DIR = ROOT_DIR / "checkpoints"

for d in (RAW_DIR, PROC_DIR, CACHE_DIR, CKPT_DIR):
    d.mkdir(parents=True, exist_ok=True)


# tqdm gives a clean progress bar (count, %, elapsed, ETA). It is optional:
# if it is not installed, we fall back to a simple "Enriched X/total" counter.
try:
    from tqdm import tqdm
    _HAS_TQDM = True
except ImportError:
    _HAS_TQDM = False

# ---------------------------------------------------------------------------
# API configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://gtr.ukri.org/gtr/api/projects"
HEADERS = {
    "Accept": "application/vnd.rcuk.gtr.json-v7",
    "User-Agent": "DurhamMDS-CE-ResearchProject/1.0 (academic use)",
}

# ---------------------------------------------------------------------------
# CACHE
# ---------------------------------------------------------------------------

CACHE_DB = CACHE_DIR / "gtr_project_cache.db"
conn = sqlite3.connect(CACHE_DB)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS api_cache (
    url TEXT PRIMARY KEY,
    response TEXT NOT NULL
)
""")
conn.commit()

CACHE_BUFFER = []
CACHE_BUFFER_SIZE = 200

# ---------------------------------------------------------------------------
# CACHE HELPERS
# ---------------------------------------------------------------------------

def get_cache(url):
    cursor.execute(
        "SELECT response FROM api_cache WHERE url = ?",
        (url,)
    )
    row = cursor.fetchone()
    return json.loads(row[0]) if row else None


def flush_cache():
    if not CACHE_BUFFER:
        return

    cursor.executemany("""
        INSERT OR REPLACE INTO api_cache (url, response)
        VALUES (?, ?)
    """, CACHE_BUFFER)

    conn.commit()
    CACHE_BUFFER.clear()

def save_cache(url, data):
    CACHE_BUFFER.append((url, json.dumps(data)))

    if len(CACHE_BUFFER) >= CACHE_BUFFER_SIZE:
        flush_cache()

# ---------------------------------------------------------------------------
# Stage 1 - identification: search terms sent to the GtR API
# ---------------------------------------------------------------------------
DEFAULT_TERMS = [
    "circular econom*",
    "industrial symbiosis", 
    "urban min*",
    "remanufactur*",
    "circular bioeconom*",
    "cradle-to-cradle",
    "closed loop",
    "circular business",
    "circular product",
    "circular industry",
    "circular management",
    "circular value chain",
    "circular transition",
    "circular supply chain",
    "waste recovery",
    "waste renewal",
    "regenerative design",
    "regenerative econom*"
]

# ---------------------------------------------------------------------------
# Master output schema
# ---------------------------------------------------------------------------
# The master file (output 1) carries only these analysis-ready columns, in this
# order. flatten_project still produces the fuller record - the screening audit
# trail plus the longer text fields - and that is retained in the audit file
# (output 2) and the validation sample (output 3), so nothing is lost, it is
# just kept out of the file used for analysis.
MASTER_COLS = [
    "project_id",
    "grant_reference",
    "title",
    "abstract_text",
    "lead_funder",
    "value_gbp",
    "start_date",
    "end_date",
    "grant_category",
    "status",
    "lead_organisation",
    "participant_organisations",
    "principal_investigator",
    "research_subjects",
    "research_topics",
    "gtr_url",
]

# ---------------------------------------------------------------------------
# Stage 2 - screening vocabulary (three tiers)
# ---------------------------------------------------------------------------
# Tiers are ordered by how reliably a term signals circular economy. The
# inclusion rule (in classify_ce) is:
#   INCLUDE if  (>=1 Tier 1 term)
#           OR  (>=2 distinct Tier 2 terms)
#           OR  (>=1 Tier 2 term AND >=1 Tier 3 term)
# Tier 3 terms never trigger inclusion on their own, only as corroboration.

# Tier 1 - DEFINING terms. A single match is sufficient for inclusion.
# These are essentially unique to circular economy discourse. The set is
# deliberately aligned with the Stage 1 search terms (a term reliable enough to
# search on is reliable enough to include on), plus two equally-specific terms
# (cradle-to-cradle, reverse logistics) that the EMF glossary (2026) defines. The
# "circular <X> economy" patterns capture domain-qualified variants such as
# "circular textile economy" or "circular plastics economy", which name the
# concept just as explicitly as the bare phrase.
TIER1_PATTERNS = [
    r"circular econom\w*",
    r"circular \w+ econom\w*",
    r"circular \w+ \w+ econom\w*",
    r"industrial symbiosis",
    r"urban mining",
    r"remanufactur\w*",
    r"circular bioeconom\w*",
    r"cradle[\s-]to[\s-]cradle",
    r"reverse logistics",
]

# Tier 2 - CHARACTERISTIC terms. Two or more DISTINCT matches are required for
# inclusion. These are genuine CE concepts (Kirchherr 2017; EMF glossary 2026)
# that nonetheless see some cross-domain use, so a single occurrence is not
# decisive but two independent ones are. This list was purified iteratively:
# terms that proved too ambiguous on inspection (closed-loop -> control
# engineering; repurpose -> generic method-repurposing; anaerobic digestion ->
# bioprocess/synthetic-biology) were demoted to Tier 3.
TIER2_PATTERNS = [
    r"circularity",
    r"technical cycle",
    r"biological cycle",
    r"refurbish\w*",
    r"secondary material\w*",
    r"non-virgin",
    r"virgin material\w*",
    r"design out waste",
    r"regenerative production",
    r"regenerati\w*\s+(?:nature|natural system\w*|design)",
    r"industrial ecology",
]

# Tier 3 - SUPPLEMENTARY terms. These never trigger inclusion on their own,
# however many match. They are the terms with the widest currency outside the
# field, so a match carries the least evidential weight. The literature places
# recycling as a last-resort action (EMF glossary 2026) and holds that a firm
# focused on recycling alone is not circular (Kirchherr 2017). A Tier 3 term
# counts only when it
# corroborates at least one Tier 2 term (see classify_ce). The demoted
# ambiguous terms (closed-loop, repurpose, anaerobic digestion) live here.
TIER3_PATTERNS = [
    r"recycl\w*",
    r"reus\w*",
    r"re-us\w*",
    r"repair\w*",
    r"recover\w*",
    r"repurpos\w*",
    r"closed[\s-]loop",
    r"anaerobic digestion",
    r"waste hierarchy",
    r"resource efficien\w*",
]

# ---------------------------------------------------------------------------
# Funder -> broad discipline mapping
# ---------------------------------------------------------------------------
FUNDER_TO_DISCIPLINE = {
    "EPSRC": "Engineering & Physical Sciences",
    "BBSRC": "Biological Sciences",
    "NERC": "Environmental Sciences",
    "ESRC": "Economic & Social Sciences",
    "AHRC": "Arts & Humanities",
    "MRC": "Medical Research",
    "STFC": "Science & Technology Facilities",
    "Innovate UK": "Industry / Applied",
    "Horizon Europe Guarantee": "International (Horizon Europe)",
    "ISCF": "Industrial Strategy Challenge Fund",
    "SPF": "Strategic Priorities Fund",
    "UKRI FLF": "Future Leaders Fellowship",
    "ISPF": "International Science Partnerships Fund",
    "Ayrton Fund": "International Development (Ayrton)",
    "COVID": "COVID Response",
    "Other NPIF": "National Productivity Investment Fund",
}

# Link rels that point to entities (people, orgs, funding) rather than
# outcomes. Sectors live on outcome records, so we skip these when gathering.
NON_OUTCOME_RELS = {
    "LEAD_ORG", "PI_PER", "FUND", "COLLAB_ORG", "PP_ORG", "FELLOW_PER",
    "CO_INV_PER", "PM_PER", "RESEARCH_PER", "STUDENT_PER", "PARTICIPANT_ORG",
    "KEY_FINDING", "IMPACT_SUMMARY", "COI_PER", "STUDENTSHIP_FROM", 
    "RESEARCH_COI_PER", "TGH_PER", "STUDENT_PP_ORG", "COFUND_ORG",
    "TRANSFER", "TRANSFER_FROM"
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def slugify(text):
    """Turn 'circular economy' into 'circular_economy' for safe filenames."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def format_with_pct(items):
    """Turn [{text, percentage}, ...] -> 'Energy (50%); ICT (50%)' style string."""
    parts = []
    for item in items:
        text = item.get("text", "").strip()
        pct = item.get("percentage")
        if text and pct is not None:
            parts.append(f"{text} ({pct}%)")
        elif text:
            parts.append(text)
    return "; ".join(parts)


def clean_text(value):
    """Normalise a GtR text field to a single-line string."""
    return (value or "").replace("\n", " ").replace("\r", " ").strip()


def get_grant_ref(identifiers):
    """Extract the primary grant reference (e.g. 'EP/V042432/1') from a
    project's identifiers list. Prefers the RCUK type, falls back to the first
    available. This is the human-readable ID the GtR website uses in URLs."""
    if not identifiers:
        return ""
    for ident in identifiers:
        if ident.get("type") == "RCUK":
            return ident.get("value", "")
    return identifiers[0].get("value", "")


def drop_internal(frame):
    """Remove internal API href columns before exporting."""
    return frame.drop(columns=[c for c in frame.columns if c.startswith("_")],
                        errors="ignore")

# Make sure the list-valued href columns are real lists (whether we just
# built them or reloaded them from the checkpoint CSV).
def _as_list(v):
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v:
        try:
            out = json.loads(v)
            return out if isinstance(out, list) else []
        except json.JSONDecodeError:
            return []
    return []


# ---------------------------------------------------------------------------
# Stage 2 screening
# ---------------------------------------------------------------------------

def find_matches(text, patterns):
    """Return the subset of patterns that match somewhere in text."""
    found = []
    for pat in patterns:
        if re.search(r"\b" + pat, text, re.IGNORECASE):
            found.append(pat)
    return found


def classify_ce(title, abstract, tech_abstract="", potential_impact=""):
    """Apply the three-tier inclusion rule across all available text fields.

    Inclusion rule:
        INCLUDE if  (>=1 Tier 1 term)
                OR  (>=2 distinct Tier 2 terms)
                OR  (>=1 Tier 2 term AND >=1 Tier 3 term)

    Tier 3 terms never trigger inclusion alone, however many match; they only
    corroborate a Tier 2 term. This encodes the literature's position that
    recycling, recovery, reuse and repair do not by themselves constitute
    circular economy.

    Returns:
        include (bool): True if the project passes Stage 2 screening
        matches (dict): which patterns matched in each tier (for audit)
    """
    text = " ".join([
        title or "", abstract or "", tech_abstract or "", potential_impact or "",
    ])
    tier1 = find_matches(text, TIER1_PATTERNS)
    tier2 = find_matches(text, TIER2_PATTERNS)
    tier3 = find_matches(text, TIER3_PATTERNS)

    include = (
        len(tier1) >= 1
        or len(tier2) >= 2
        or (len(tier2) >= 1 and len(tier3) >= 1)
    )
    return include, {"tier1": tier1, "tier2": tier2, "tier3": tier3}


# ---------------------------------------------------------------------------
# API fetchers
# ---------------------------------------------------------------------------
# The GtR API can wobble on long runs (read timeouts, transient 502/503/504
# server errors). These are not permanent failures, so rather than giving up we
# retry with exponential backoff. A 404 (page genuinely absent) or 403/4xx is
# NOT retried, since retrying would not help.

# HTTP status codes that are worth retrying (server-side / transient).
RETRYABLE_STATUS = {500, 502, 503, 504, 429}

# Tunable retry behaviour (overridable from the CLI via globals set in main()).
MAX_RETRIES = 8          # attempts per request before giving up
BACKOFF_BASE = 3.0       # seconds; wait grows 3, 9, 27, 81, 243 ... between
                         # attempts, so a term survives roughly ten minutes of
                         # GtR being unwell rather than one.


def _request_with_retries(session, url, params=None, headers=None, max_retries=None, 
                          backoff_base=None, retryable_status = None):
    """GET a URL with retry-and-backoff on timeouts and transient server errors.

    Returns the parsed JSON on success. Raises the last exception if every
    attempt fails, so the caller can decide whether to skip or abort.
    """
    if headers is None:
        headers = {}
    if max_retries is None:
        max_retries = MAX_RETRIES
    if backoff_base is None:
        backoff_base = BACKOFF_BASE
    if retryable_status is None:
        retryable_status = RETRYABLE_STATUS
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, params=params, timeout=60)
            # Retry on transient server statuses; raise on other 4xx/5xx.
            if resp.status_code in retryable_status:
                raise requests.HTTPError(
                    f"{resp.status_code} (retryable) for {resp.url}", response=resp)
            resp.raise_for_status()
            return resp.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            # Only retry timeouts, connection drops, and retryable statuses.
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = (
                isinstance(exc, (requests.Timeout, requests.ConnectionError))
                or status in retryable_status
            )
            if not retryable or attempt == max_retries:
                raise
            # Testing
            if status == 429:
                retry_after = exc.response.headers.get("Retry-After")
                if retry_after:
                    wait = float(retry_after)
                else:
                    wait = backoff_base * (2 ** (attempt - 1))
            else:
                wait = backoff_base * (2 ** (attempt - 1))
            print(f"\n    (attempt {attempt}/{max_retries} failed: {exc}; "
                  f"retrying in {wait:.0f}s)", flush=True)
            time.sleep(wait)


def fetch_page(query, page, size, session, delay):
    """Fetch a single page of GtR project search results for a keyword term."""
    params = {"q": query, "p": page, "s": size}
    data = _request_with_retries(session, BASE_URL, params=params, headers=HEADERS, 
                                 max_retries=MAX_RETRIES, backoff_base=BACKOFF_BASE,
                                 retryable_status=RETRYABLE_STATUS)
    time.sleep(delay)
    return data


def fetch_json(href, session, delay):
    """Generic helper: fetch any GtR API URL and return parsed JSON.
    Retries transient failures; returns {} only if all retries are exhausted,
    so enrichment of a single project can fail softly without killing the run."""
    if not href:
        return {}

    # Check SQLite cache first
    cached = get_cache(href)
    if cached is not None:
        return cached

    try:
        data = _request_with_retries(session, href, headers=HEADERS, 
                                     max_retries=MAX_RETRIES, backoff_base=BACKOFF_BASE,
                                     retryable_status=RETRYABLE_STATUS)
        time.sleep(delay)
        # Save to cache
        save_cache(href, data)
        return data

    except requests.RequestException:
        return {}


def fetch_fund_value(fund_href, session, delay):
    """Extract funding amount (GBP) from a fund endpoint."""
    if not fund_href:
        return ""
    data = fetch_json(fund_href, session, delay)
    vp = data.get("valuePounds")
    if isinstance(vp, dict):
        return vp.get("amount", "")
    return vp if vp is not None else ""


def fetch_org_name(org_href, session, delay):
    """Resolve organisation name from GtR organisation endpoint."""
    if not org_href:
        return ""
    data = fetch_json(org_href, session, delay)
    return data.get("name", "")


def fetch_person_name(person_href, session, delay):
    """Resolve PI name from GtR person endpoint."""
    if not person_href:
        return ""
    data = fetch_json(person_href, session, delay)
    first = data.get("firstName", "") or ""
    other = data.get("otherNames", "") or ""
    surname = data.get("surname", "") or ""
    return " ".join(p for p in [first, other, surname] if p).strip()


def fetch_sectors_from_hrefs(hrefs, session, delay):
    """Collect impact sectors from a project's outcome records.
    Sectors are tagged on outcome records (mainly key findings), not on the
    project record itself. Values are captured raw (GtR splits some names on
    internal commas and has occasional source typos); normalisation happens
    later in processing. Returns a semicolon-joined string of unique sectors."""
    if not hrefs:
        return ""
    sectors = []
    seen = set()
    for href in hrefs:
        if not href:
            continue
        data = fetch_json(href, session, delay)
        # safely get sectors block
        sec = data.get("sectors")
        if isinstance(sec, dict):
            for item in sec.get("item", []):
                val = (item or "").strip()
                if val and val not in seen:
                    seen.add(val)
                    sectors.append(val)
    return "; ".join(sectors) if sectors else ""


# ---------------------------------------------------------------------------
# Per-term collection
# ---------------------------------------------------------------------------

def _replay_raw(raw_path):
    """Yield projects from a previously completed raw file."""
    with open(raw_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def collect_query(term, size, max_pages, session, delay, raw_path):
    """Collect all pages for one search term, writing the raw JSON to disk
    incrementally (one project per line, JSONL) so nothing is held in memory
    longer than needed and a partial run still leaves a usable raw file.

    If a completed raw file for this term already exists, it is replayed from
    disk instead of refetched. Completion is signalled by a .done marker
    written only after the final page, so a truncated file is never reused.
    """
    done_marker = raw_path.with_suffix(raw_path.suffix + ".done")
    cached = sorted(raw_path.parent.glob(f"{raw_path.stem.rsplit('_', 2)[0]}_*.jsonl.done"))
    if cached:
        source = cached[-1].with_suffix("")
        if source.exists():
            print(f"\nSearch term: '{term}' (replaying {source.name}, already complete)")
            yield from _replay_raw(source)
            return

    print(f"\nSearch term: '{term}'\n")
    first = fetch_page(term, 1, size, session, delay)
    total_pages = first.get("totalPages", 1)

    if max_pages:
        total_pages = min(total_pages, max_pages)
        print(f"    (limited to {total_pages} page(s) for this run)")

    with open(raw_path, "w", encoding="utf-8") as fh:
        projects = list(first.get("project", []))
        for p in projects:
            fh.write(json.dumps(p, ensure_ascii=False) + "\n")
            yield p

        for page in range(2, total_pages + 1):
            try:
                data = fetch_page(term, page, size, session, delay)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    break
                raise
            page_projects = data.get("project", [])
            for p in page_projects:
                fh.write(json.dumps(p, ensure_ascii=False) + "\n")
                yield p
            print(f"    page {page}/{total_pages} collected", end="\r")

    # Every page for this term is on disk, so mark it replayable.
    done_marker.write_text("complete\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Project flattening (no API calls - just reshapes the search response and
# stashes the hrefs that enrichment will need later)
# ---------------------------------------------------------------------------


def flatten_project(project, search_term):
    """Flatten nested GtR project JSON into a single structured row."""
    subjects = project.get("researchSubjects", {}).get("researchSubject", [])
    topics = project.get("researchTopics", {}).get("researchTopic", [])
    links = project.get("links", {}).get("link", [])

    fund_links, pi_links, lead_org_links, participant_org_links, key_finding_href, outcome_links = [], [], [], [], [], []

    for lk in links:
        href = lk.get("href", "")
        rel = lk.get("rel", "")
        if not href:
            continue
        if rel == "FUND":
            fund_links.append(lk)
        elif rel == "PI_PER":
            pi_links.append(lk)
        elif rel == "LEAD_ORG":
            lead_org_links.append(lk)
        elif rel == "PARTICIPANT_ORG":
            participant_org_links.append(lk)
        elif rel == "KEY_FINDING":
            key_finding_href.append(href)
        elif rel not in NON_OUTCOME_RELS:
            outcome_links.append(lk)

    fund_link = fund_links[0] if fund_links else {}
    fund_href = fund_link.get("href", "")
    pi_href = pi_links[0].get("href", "") if pi_links else ""
    lead_org_href = lead_org_links[0].get("href", "") if lead_org_links else ""
    participant_org_href = [lk.get("href") for lk in participant_org_links]
    outcome_href = [lk.get("href") for lk in outcome_links]

    # Discipline signal (no API call - uses fields already in the response)
    subjects_with_pct = [s for s in subjects if s.get("percentage", 0) > 0]
    research_subjects_str = format_with_pct(subjects)
    lead_funder = project.get("leadFunder", "")
    if subjects_with_pct:
        discipline_primary = research_subjects_str
        discipline_source = "research_subjects"
    else:
        discipline_primary = FUNDER_TO_DISCIPLINE.get(lead_funder, lead_funder)
        discipline_source = "funder_mapping"

    title = project.get("title", "")
    abstract = clean_text(project.get("abstractText"))
    tech_abstract = clean_text(project.get("techAbstractText"))
    potential_impact = clean_text(project.get("potentialImpact"))

    include, matches = classify_ce(title, abstract, tech_abstract, potential_impact)
    project_id = project.get("id", "")

    identifiers_list = project.get("identifiers", {}).get("identifier", [])
    grant_ref = get_grant_ref(identifiers_list)
    gtr_url = (
        f"https://gtr.ukri.org/projects?ref={quote(grant_ref, safe='')}"
        if grant_ref
        else f"https://gtr.ukri.org/projects?ref={project_id}"
    )

    return {
        "project_id": project_id,
        "title": title,
        # Enrichment fields - filled later by enrich_row (blank for now)
        "lead_organisation": "",
        "participant_organisations": "",
        "principal_investigator": "",
        "value_gbp": "",
        "funding_data_available": "",
        "sectors": "",
        # Fields available directly from the search response
        "lead_funder": lead_funder,
        "start_date": fund_link.get("start"),
        "end_date": fund_link.get("end"),
        "status": project.get("status", ""),
        "grant_category": project.get("grantCategory", ""),
        "grant_reference": grant_ref,
        "discipline_primary": discipline_primary,
        "discipline_source": discipline_source,
        "research_subjects": research_subjects_str,
        "research_topics": format_with_pct(topics),
        "n_research_subjects": len(subjects_with_pct),
        "abstract_text": abstract,
        "tech_abstract_text": tech_abstract,
        "potential_impact": potential_impact,
        "gtr_url": gtr_url,
        "matched_search_terms": "",  # filled after the union with the retrieving terms
        "filter_decision": "keep" if include else "drop",
        "tier1_matches": "; ".join(matches["tier1"]),
        "tier2_matches": "; ".join(matches["tier2"]),
        "tier3_matches": "; ".join(matches["tier3"]),
        # Internal href fields (stripped before output)
        "_fund_href": fund_href,
        "_pi_href": pi_href,
        "_lead_org_href": lead_org_href,
        "_participant_org_href": participant_org_href,
        "_outcome_href": outcome_href,
        "_key_finding_href": key_finding_href
    }



# ---------------------------------------------------------------------------
# Enrichment (runs after deduplication + screening, so only on unique keepers)
# ---------------------------------------------------------------------------

def enrich_row(row, delay, session, collect_sectors, organisation_lookup, person_lookup, fund_lookup,):
    """Add fund value, PI name, organisation names, and (optionally) sectors.
    Enrichment runs after deduplication and filtering so we only make API calls
    for genuinely CE-relevant, unique projects."""
    lead_org_name = organisation_lookup.get(row.get("_lead_org_href", ""),"")
    row["lead_organisation"] = lead_org_name

    # Funding value, plus a flag marking whether GtR actually holds a value
    # (studentships and some other categories carry no per-project funding).
    fund_value = fund_lookup.get(row.get("_fund_href", ""),"")
    row["value_gbp"] = fund_value
    row["funding_data_available"] = bool(
        fund_value and str(fund_value) not in ("", "0", "0.0"))

    row["principal_investigator"] = person_lookup.get(row.get("_pi_href", ""), "")

    # Participant organisations, deduplicated against the lead org
    participant_href = row.get("_participant_org_href", []) or []
    names = [organisation_lookup.get(href, "") for href in participant_href
             if href]
    participant_set = {org.strip() for org in names if org and org.strip()}
    participant_set.discard((lead_org_name or "").strip())
    row["participant_organisations"] = "; ".join(sorted(participant_set))

    # Impact sectors (opt-in via --sectors; follows outcome records)
    if collect_sectors:
        row["sectors"] = fetch_sectors_from_hrefs(
            row.get("_key_finding_href", []) or [], session, delay)
    return row

def enrich_dataset(kept_df, session, delay, collect_sectors, organisation_lookup,
    person_lookup, fund_lookup, tqdm_enabled=False):
    """
    Builds lookup tables (if not provided) and enriches kept_df in one pass.
    Returns enriched DataFrame + lookup caches.
    """
    # Collect unique URLs before enriching
    print("\nCollecting unique lookup URLs...")
    org_urls = set()
    person_urls = set()
    fund_urls = set()
    for _, row in kept_df.iterrows():
        if row.get("_lead_org_href"):
            org_urls.add(row["_lead_org_href"])
        for href in row.get("_participant_org_href", []) or []:
            if href:
                org_urls.add(href)
        if row.get("_pi_href"):
            person_urls.add(row["_pi_href"])
        if row.get("_fund_href"):
            fund_urls.add(row["_fund_href"])
    print(f"    Organisations: {len(org_urls)}")
    print(f"    People:        {len(person_urls)}")
    print(f"    Funds:         {len(fund_urls)}")

    # Build lookup tables (only fetch missing entries)
    if organisation_lookup is None:
        organisation_lookup = {}
    print("\nFetching organisations...")
    iterator = tqdm(org_urls, desc="Organisations") if tqdm_enabled else org_urls
    for href in iterator:
        if href not in organisation_lookup:
            organisation_lookup[href] = fetch_org_name(href, session, delay)
    
    if person_lookup is None:
        person_lookup = {}
    print("Fetching people...")
    iterator = tqdm(person_urls, desc="People") if tqdm_enabled else person_urls
    for href in iterator:
        if href not in person_lookup:
            person_lookup[href] = fetch_person_name(href, session, delay)

    if fund_lookup is None:
        fund_lookup = {}
    print("Fetching funds...")
    iterator = tqdm(fund_urls, desc="Funds") if tqdm_enabled else fund_urls
    for href in iterator:
        if href not in fund_lookup:
            fund_lookup[href] = fetch_fund_value(href, session, delay)

    # Apply enrichment
    print("\nEnriching rows...")
    enriched_rows = []
    iterator = kept_df.iterrows()
    if tqdm_enabled:
        iterator = tqdm(iterator, total=len(kept_df), desc="Enriching")
    for _, row in iterator:
        enriched_rows.append(enrich_row(row.to_dict(), delay, session,
                                        collect_sectors, organisation_lookup,
                                        person_lookup, fund_lookup))
    return pd.DataFrame(enriched_rows), organisation_lookup, person_lookup, fund_lookup


# ---------------------------------------------------------------------------
# Checkpoint helpers (resume support for the slow enrichment phase)
# ---------------------------------------------------------------------------

def load_checkpoint(enriched_path):
    """Return (list of already-enriched row dicts, set of their project_ids).
    Reads the JSONL checkpoint if it exists, else returns empties."""
    rows, done = [], set()
    if enriched_path.exists():
        with open(enriched_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue  # skip a half-written final line
                rows.append(rec)
                done.add(str(rec.get("project_id", "")))
    return rows, done


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Collect CE projects from the UKRI GtR API.")
    parser.add_argument("--terms", type=str, default=None, 
                        help="Overwrite default search terms (separate by ',')")
    parser.add_argument("--size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between API calls (default 0.3; raise to be gentler)")
    parser.add_argument("--out-dir", type=str, default=None)
    parser.add_argument("--no-enrich", action="store_true",
                        help="Skip fund/org/person lookups (faster, less data)")
    parser.add_argument("--no-filter", action="store_true",
                        help="Skip the CE screening (keep all matches)")
    parser.add_argument("--sectors", action="store_true",
                        help="Also collect impact sectors from outcomes (slow)")
    parser.add_argument("--outcome-links", action="store_true", 
                        help="Also collect all outcomes hrefs")
    parser.add_argument("--validation-size", type=int, default=100,
                        help="Total hand-coding sample size, split evenly "
                             "between the rule's keep and drop strata")
    parser.add_argument("--checkpoint-every", type=int, default=100,
                        help="Save enrichment progress every N projects (default 100)")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignore any existing checkpoint and start enrichment over")
    parser.add_argument("--max-retries", type=int, default=6,
                        help="Retries per request on timeout/server error (default 5)")
    parser.add_argument("--backoff-base", type=float, default=2.0,
                        help="Base seconds for exponential backoff between retries (default 2)")
    args = parser.parse_args()

    # Wire retry settings into the module-level globals the fetchers use.
    global MAX_RETRIES, BACKOFF_BASE
    MAX_RETRIES = max(1, args.max_retries)
    BACKOFF_BASE = max(0.0, args.backoff_base)

    size = max(10, min(args.size, 100))
    # Combine either default or user-entered terms into one search query
    terms = (
        [t.strip() for t in args.terms.split(",") if t.strip()]
        if args.terms else DEFAULT_TERMS
    )
    search_query = " OR ".join(terms)
    enrich = not args.no_enrich
    apply_filter = not args.no_filter
    collect_sectors = args.sectors
    collect_outcomes = args.outcome_links

    # Checkpoint files are keyed to the run configuration (search terms + page size
    # + sector setting) with a hash key
    terms_hash = hashlib.md5(search_query.encode()).hexdigest()[:8]
    run_key = f"{terms_hash}_s{size}_sec{int(collect_sectors)}"
    screened_ckpt = CKPT_DIR / f"screened_{run_key}.csv"
    enriched_ckpt = CKPT_DIR / f"enriched_{run_key}.jsonl"

    if args.fresh:
        for f in (screened_ckpt, enriched_ckpt):
            if f.exists():
                f.unlink()
        print("Starting fresh: existing checkpoint cleared.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session = requests.Session()

    print("=" * 64)
    print("UKRI Gateway to Research - circular economy project collection")
    print("=" * 64)
    print(f"Terms: {len(terms)} | Page size: {size} | Delay: {args.delay}s")
    print(f"Enrich: {enrich} | CE screening: {apply_filter} | Sectors: {collect_sectors}")
    if not _HAS_TQDM:
        print("(tqdm not installed - using a simple counter; "
              "pip install tqdm for a progress bar)")

    # -----------------------------------------------------------------------
    # Phase 1: collect + screen. If a screened checkpoint already exists for
    # this exact run, reload it and skip the searching entirely.
    # -----------------------------------------------------------------------
    if screened_ckpt.exists() and not args.fresh:
        print(f"\nReloading screened set from checkpoint: {screened_ckpt.name}")
        all_df = pd.read_csv(screened_ckpt)
        # Re-derive the href columns is not possible from CSV (they were
        # internal), so the screened checkpoint stores them too - see below.
        print(f"  Unique projects in checkpoint: {len(all_df)}")
    else:
        # Each term is queried SEPARATELY and results are unioned locally on
        # project_id. A single combined OR query returns >100k records over
        # ~1,100 pages of a relevance-ranked response, and deep pagination of
        # that response proved unstable (projects observably present in the
        # database were missing from late pages). Per-term queries keep each
        # pagination shallow, make retrieval reproducible, and give exact
        # per-term provenance for free. The abort guard still covers the whole
        # run: if any term fails after retries, nothing is written.
        seen = {}            # project_id -> flattened row (first sighting)
        provenance = {}      # project_id -> set of retrieving terms
        n_raw_total = 0
        try:
            for term in terms:
                raw_path = RAW_DIR / f"gtr_raw_{slugify(term)}_{timestamp}.jsonl"
                n_raw = n_new = 0
                for p in collect_query(
                        term,
                        size,
                        args.max_pages,
                        session,
                        args.delay,
                        raw_path):
                    pid = p.get("id", "")
                    n_raw += 1
                    provenance.setdefault(pid, set()).add(term)
                    if pid not in seen:
                        seen[pid] = flatten_project(p, term)
                        n_new += 1
                n_raw_total += n_raw
                print(f"    '{term}': {n_raw} records, {n_new} new unique")
        except requests.RequestException as exc:
            # A term failing AFTER all retries means the API is genuinely
            # unavailable. Writing partial output here is the dangerous
            # failure mode (a "complete"-looking file missing whole terms),
            # so we abort loudly and write nothing. Nothing is lost: rerun
            # the same command when the API is back and it starts cleanly.
            print("\n" + "=" * 64, file=sys.stderr)
            print(f"ABORTING: collection failed on term '{term}' "
                f"after {MAX_RETRIES} retries.",
                file=sys.stderr)
            print(f"Reason: {exc}", file=sys.stderr)
            print("The GtR API appears to be unavailable or rate-limiting. "
                    "No output\nor checkpoint has been written. Wait a few "
                    "minutes and rerun the\nsame command - it will start "
                    "cleanly.", file=sys.stderr)
            print("=" * 64, file=sys.stderr)
            sys.exit(1)

        if not seen:
            print("\nNo projects collected. Check search terms or connection.")
            sys.exit(1)

        for pid, row in seen.items():
            row["matched_search_terms"] = "; ".join(sorted(provenance[pid]))
        all_rows = list(seen.values())

        all_df = pd.DataFrame(all_rows).drop_duplicates(subset="project_id").reset_index(drop=True)

        print(f"\nRecords identified from GtR searches: {n_raw_total}")
        print(f"Duplicate records removed: {n_raw_total - len(all_df)}")
        print(f"Unique projects collected: {len(all_df)}")

        # Save the screened checkpoint INCLUDING the internal href columns,
        # so a resumed enrichment run has everything it needs without
        # re-searching. (Lists are JSON-encoded to survive the CSV round-trip.)
        ckpt_df = all_df.copy()
        ckpt_df["_participant_org_href"] = ckpt_df["_participant_org_href"].apply(json.dumps)
        ckpt_df["_key_finding_href"] = ckpt_df["_key_finding_href"].apply(json.dumps)
        ckpt_df["_outcome_href"] = ckpt_df["_outcome_href"].apply(json.dumps)
        ckpt_df.to_csv(screened_ckpt, index=False, encoding="utf-8")
        print(f"\nScreened set saved to checkpoint: {screened_ckpt.name}")

    if "_participant_org_href" in all_df.columns:
        all_df["_participant_org_href"] = all_df["_participant_org_href"].apply(_as_list)
    if "_key_finding_href" in all_df.columns:
        all_df["_key_finding_href"] = all_df["_key_finding_href"].apply(_as_list)
    if "_outcome_href" in all_df.columns:
        all_df["_outcome_href"] = all_df["_outcome_href"].apply(_as_list)

    if apply_filter:
        kept_df = all_df[all_df["filter_decision"] == "keep"].copy()
        excluded_df = all_df[all_df["filter_decision"] == "drop"].copy()
    else:
        kept_df = all_df.copy()
        excluded_df = pd.DataFrame()

    automated_excluded = len(excluded_df)
    print("\nScreening counts:")
    print(f"    Records screened: {len(all_df)}")
    print(f"    Automatically excluded: {automated_excluded}")
    print(f"    Automatically retained: {len(kept_df)}")

    # -----------------------------------------------------------------------
    # Phase 2: enrich only the unique keepers, checkpointing as we go.
    # -----------------------------------------------------------------------
    if enrich and len(kept_df) > 0:
        enriched_rows, done_ids = load_checkpoint(enriched_ckpt)
        if args.fresh:
            enriched_rows = []
            done_ids = set()
        todo_df = kept_df[~kept_df["project_id"].astype(str).isin(done_ids)].copy()
        print(f"\nEnrichment: {len(done_ids)} already done, {len(todo_df)} remaining")
        organisation_lookup = {}
        person_lookup = {}
        fund_lookup = {}

        if len(todo_df) > 0:
            # Build lookup tables
            enriched_df, organisation_lookup, person_lookup, fund_lookup = enrich_dataset(
                kept_df=todo_df,
                session=session,
                delay=args.delay,
                collect_sectors=collect_sectors,
                organisation_lookup=organisation_lookup,
                person_lookup=person_lookup,
                fund_lookup=fund_lookup,
                tqdm_enabled=_HAS_TQDM
            )

            # Checkpoint writing
            ckpt_fh = open(enriched_ckpt, "a", encoding="utf-8")
            try:
                iterator = enriched_df.iterrows()
                if _HAS_TQDM:
                    iterator = tqdm(iterator, total=len(enriched_df), desc="Checkpointing")
                processed_since_flush = 0
                for _, row in iterator:
                    enriched = row  # already enriched by enrich_dataset
                    enriched_rows.append(enriched)
                    ckpt_fh.write(json.dumps(enriched.to_dict(), ensure_ascii=False) + "\n")
                    processed_since_flush += 1
                    if processed_since_flush >= args.checkpoint_every:
                        ckpt_fh.flush()
                        processed_since_flush = 0
            finally:
                ckpt_fh.flush()
                ckpt_fh.close()
            kept_df = pd.DataFrame(enriched_rows)
        else:
            print("Nothing left to enrich.")
            kept_df = pd.DataFrame(enriched_rows)
        print(f"\nEnrichment complete: {len(kept_df)} total rows")

    # ---- Optional: Collect outcomes (opt-in via --outcomes) ----
    outcome_rows = []
    if collect_outcomes:
        print("\nFetching outcome links...")
        for _, row in kept_df.iterrows():
            for href in row["_outcome_href"]:
                outcome_rows.append({
                    "project_id": row["project_id"],
                    "grant_reference": row["grant_reference"],
                    "title": row["title"],
                    "outcome_href": href,
                })
        outcome_path = RAW_DIR / f"gtr_outcome_hrefs.csv"
        pd.DataFrame(outcome_rows).to_csv(outcome_path, index=False, encoding="utf-8")
        print(f"    Saved {len(outcome_rows)} outcome links to {outcome_path}")

    kept_df = drop_internal(kept_df)
    all_df_out = drop_internal(all_df)

    # ---- Output 1: kept projects, MASTER_COLS only ----
    # Selected defensively: a column absent from this run is skipped rather
    # than raising, and any missing one is named so a silent schema change
    # cannot pass unnoticed.
    out_path = PROC_DIR / f"gtr_projects_{timestamp}.csv"
    latest_path = PROC_DIR / "gtr_projects_latest.csv"
    master_cols = [c for c in MASTER_COLS if c in kept_df.columns]
    absent = [c for c in MASTER_COLS if c not in kept_df.columns]
    if absent:
        print(f"    NOTE: master columns not produced this run: {absent}")
    kept_master = kept_df[master_cols]
    kept_master.to_csv(out_path, index=False, encoding="utf-8")
    kept_master.to_csv(latest_path, index=False, encoding="utf-8")
    print(f"    Master file: {len(master_cols)} columns "
          f"(of {len(kept_df.columns)} collected)")

    # ---- Output 2: full set with filter decisions (audit) ----
    all_path = PROC_DIR / f"gtr_all_with_decision_{timestamp}.csv"
    all_df_out.to_csv(all_path, index=False, encoding="utf-8")

    # ---- Output 3: validation sample for hand-coding ----
    # Draw evenly from the keep and drop strata so precision and NPV are separately estimable.
    half = args.validation_size // 2
    strata = []
    for decision in ("keep", "drop"):
        pool = all_df_out[all_df_out["filter_decision"] == decision]
        take = min(half, len(pool))
        if take < half:
            print(f"  WARNING: only {len(pool)} '{decision}' rows available, "
                  f"wanted {half}. Precision/NPV interval will be wider.")
        strata.append(pool.sample(n=take, random_state=42))
    # Shuffle so the two strata are interleaved for the coder.
    sample = pd.concat(strata).sample(frac=1, random_state=7).copy()
    # Coerce to string first: a screened set reloaded from the checkpoint CSV
    # can carry NaN (float) in empty text cells, which would break .str.slice.
    sample["abstract_preview"] = sample["abstract_text"].fillna("").astype(str).str.slice(0, 300)
    sample["tech_abstract_preview"] = sample["tech_abstract_text"].fillna("").astype(str).str.slice(0, 300)
    sample["potential_impact_preview"] = sample["potential_impact"].fillna("").astype(str).str.slice(0, 300)
    sample["is_ce_manual"] = ""
    val_cols = [
        "project_id", "title", "matched_search_terms", "filter_decision",
        "tier1_matches", "tier2_matches", "tier3_matches",
        "abstract_preview", "tech_abstract_preview", "potential_impact_preview",
        "gtr_url", "is_ce_manual",
    ]
    val_path = PROC_DIR / f"gtr_validation_sample_{timestamp}.csv"
    sample[val_cols].to_csv(val_path, index=False, encoding="utf-8")

    print(f"\nOutputs in {PROC_DIR}/:")
    print(f"    {out_path.name}            (kept projects)")
    print(f"    {all_path.name}   (all projects + screening decision)")
    print(f"    {val_path.name}   (hand-code: fill is_ce_manual with "
          f"keep/drop/unsure; stratified {half}+{half})")
    print(f"\nCheckpoints in {CKPT_DIR}/ (safe to delete now this run finished cleanly).")
    print("\nDone.")

    flush_cache()
    conn.close()


if __name__ == "__main__":
    main()