"""
build_final_database.py
=======================
Assemble the cleaned pipeline tables into one relational database linking
research inputs (awards, funding, institutions, disciplines) to research
outputs (publications and the other outcome types).

Why this step exists
--------------------
`build_analysis_dataset.py` produced three flat CSVs and had to re-derive the
project-to-outcome links itself by stacking the four bibliographic sources.
Since then the pipeline has grown a proper merged layer: `merged/outcomes.csv`
and `merged/project_outcome_map.csv` deduplicate across GtR, OpenAlex, Scopus
and Web of Science into a single `global_outcome_id`. This script builds on
that layer instead, so the link between an input and an output is a stored
relationship rather than something each analysis re-derives.

It computes no result and makes no analytical choice. It joins, counts,
derives and records provenance. Anything that produces a finding belongs
downstream of here.

Grain, which is the thing most easily got wrong
-----------------------------------------------
`merged/outcomes.csv` is row-for-row aligned with `merged/project_outcome_map.csv`,
so it is at the PROJECT-OUTCOME-LINK grain, not the outcome grain. 612 outcomes
are linked to more than one project and therefore appear more than once, and
their attributes can differ between those rows because a shared paper may have
been matched through different sources for different projects. This script
collapses them to one row per `global_outcome_id` by taking the first non-null
value per column, and flags any outcome whose `type` was not consistent across
its link rows.

Two counting conventions are carried side by side
-------------------------------------------------
The Methodology chapter's 616 / 1,640 figure comes from the SOURCE-STACKED
definition: a project has a publication if any of the four source files
contains a publication record for it. The merged layer gives a second,
deduplicated definition. Both are computed. They are reported against each
other rather than silently reconciled, because the chapter quotes the first.

Inputs (all under the ce-gtr-data repo, data/)
----------------------------------------------
  cleaned/merged/projects.csv                     the 1,640-project spine
  cleaned/merged/outcomes.csv                     harmonised outcome records
  cleaned/merged/project_outcome_map.csv          project to outcome, with provenance
  cleaned/institutions/institutions.csv           organisation registry
  cleaned/institutions/project_institutions.csv   project to organisation, long
  classification/projects_labelled_final.csv      discipline label, source, tier
  classification/project_field_probabilities.csv  discipline probability distribution
  cleaned/outcomes/publications_labelled.csv      DOI to discipline field
  cleaned/outcomes/publication_field_probabilities.csv
  cleaned/outcomes/gtr_all_outcomes_clean.csv     twelve GtR outcome types
  cleaned/outcomes/{openalex,scopus,wos}_all_outcomes_clean.csv
  cleaned/authors/author_identities.csv           disambiguated authors
  cleaned/authors/authors_long.csv                author to outcome, long

Outputs (data/final/, gitignored like the rest of data/)
--------------------------------------------------------
  ce_ecosystem.db        SQLite, tables + indexes + views
  ce_ecosystem.xlsx      browsable workbook, one sheet per table, text trimmed
  BUILD_MANIFEST.md      every input file with its size, mtime and SHA-256

The database is 80 MB and is deliberately not committed. The script is what is
shared; run it to get the database.

Run from the repository root:
    python -m scripts.cleaning.build_final_database
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# LOCATIONS
# ---------------------------------------------------------------------------

REPO = Path(os.environ.get("CE_DATA_ROOT",
                           Path(__file__).resolve().parents[2]))
DATA = REPO / "data"
CLEAN = DATA / "cleaned"
OUT_DIR = DATA / "final"

DB_PATH = OUT_DIR / "ce_ecosystem.db"
XLSX_PATH = OUT_DIR / "ce_ecosystem.xlsx"

# The collection date. All elapsed-time fields are measured to this date so
# they are stable across runs. Change it only when the data is recollected.
REFERENCE_DATE = pd.Timestamp("2026-08-06")

# Figures committed to the Methodology chapter. The build asserts against
# these and reports drift rather than quietly publishing a second version of
# the same number.
CANONICAL = {
    "n_projects": 1640,
    "n_with_publication": 616,
    "n_without_publication": 1024,
}

# The twelve GtR outcome types, used for the source-stacked counts that the
# chapter's figures are built on.
GTR_OUTCOME_TYPES = [
    "publications", "disseminations", "collaborations", "furtherfundings",
    "policyinfluences", "researchdatabaseandmodels",
    "artisticandcreativeproducts", "researchmaterials",
    "softwareandtechnicalproducts", "intellectualproperties", "spinouts",
    "products",
]

# Types that represent a research output rather than an activity or an input.
# A subsequent grant is an input; a conference talk is an activity. Counting
# either as output would flatter the productivity figures.
NOT_OUTPUT = {"disseminations", "furtherfundings", "collaborations"}
OUTPUT_TYPES = [t for t in GTR_OUTCOME_TYPES if t not in NOT_OUTPUT]

# The same distinction in the merged layer's harmonised vocabulary.
MERGED_NOT_OUTPUT = {"dissemination", "further_funding", "collaboration"}

INPUT_FILES = {
    "projects": CLEAN / "merged" / "projects.csv",
    "merged_outcomes": CLEAN / "merged" / "outcomes.csv",
    "project_outcome_map": CLEAN / "merged" / "project_outcome_map.csv",
    "institutions": CLEAN / "institutions" / "institutions.csv",
    "project_institutions": CLEAN / "institutions" / "project_institutions.csv",
    "discipline_labels": DATA / "classification" / "projects_labelled_final.csv",
    "discipline_probabilities": DATA / "classification" / "project_field_probabilities.csv",
    "publications_labelled": CLEAN / "outcomes" / "publications_labelled.csv",
    "publication_probabilities": CLEAN / "outcomes" / "publication_field_probabilities.csv",
    "gtr_outcomes": CLEAN / "outcomes" / "gtr_all_outcomes_clean.csv",
    "openalex_outcomes": CLEAN / "outcomes" / "openalex_all_outcomes_clean.csv",
    "scopus_outcomes": CLEAN / "outcomes" / "scopus_all_outcomes_clean.csv",
    "wos_outcomes": CLEAN / "outcomes" / "wos_all_outcomes_clean.csv",
    "author_identities": CLEAN / "authors" / "author_identities.csv",
    "authors_long": CLEAN / "authors" / "authors_long.csv",
}

WARNINGS: list[str] = []


def warn(message: str) -> None:
    WARNINGS.append(message)
    print(f"  [WARN] {message}")


def normalise_doi(value) -> str | None:
    """Lower-case a DOI and strip any resolver prefix."""
    if not isinstance(value, str):
        return None
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", value.strip().lower())
    return text or None


def read(key: str, **kwargs) -> pd.DataFrame:
    path = INPUT_FILES[key]
    if not path.exists():
        raise SystemExit(f"missing input: {path}")
    return pd.read_csv(path, low_memory=False, **kwargs)


def numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    """Numeric view of a column, or an all-null column if it is absent.

    The merged outcome schema is still moving as the merge script is
    refined, and a column disappearing should degrade the affected field
    rather than abort the whole build.
    """
    if column not in frame.columns:
        warn(f"column `{column}` is no longer in the merged outcomes; "
             f"anything derived from it will be null")
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


# Columns the views select by name. A view referring to a missing column
# fails at CREATE VIEW with a bare "no such column", which is a poor way to
# discover a schema change, so check up front and say which and where.
REQUIRED_OUTCOME_COLUMNS = ["global_outcome_id", "title", "type", "subtype",
                            "year", "doi", "source_title", "cited_by"]


def check_outcome_schema(outcomes: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_OUTCOME_COLUMNS if c not in outcomes.columns]
    if missing:
        raise SystemExit(
            "the merged outcomes file no longer carries: "
            + ", ".join(missing)
            + "\nThese are selected by name in the database views. Either the "
              "merge script renamed them, or they moved to another table. "
              "Update REQUIRED_OUTCOME_COLUMNS and the VIEWS block together.")


# ---------------------------------------------------------------------------
# 1. PROJECTS, the input side
# ---------------------------------------------------------------------------

def build_projects() -> pd.DataFrame:
    """The project spine with the OpenAlex topic fields namespaced.

    `projects.csv` carries `field`, `domain` and `subfield` from OpenAlex's
    topic model, and the discipline classification carries its own `field`.
    Renaming here prevents a silent collision in the merge.
    """
    spine = read("projects").rename(columns={
        "field": "openalex_field", "domain": "openalex_domain",
        "subfield": "openalex_subfield", "primary_topic": "openalex_topic",
        "primary_topic_score": "openalex_topic_score"})
    if not spine.project_id.is_unique:
        raise SystemExit("project_id is not unique in the spine")
    return spine


def add_discipline(projects: pd.DataFrame) -> pd.DataFrame:
    labels = read("discipline_labels")
    keep = {"project_id": "project_id", "field": "discipline_field",
            "field_source": "discipline_source",
            "confidence": "discipline_confidence",
            "tier": "discipline_tier", "funder_field": "discipline_funder",
            "model_field": "discipline_model",
            "model_confidence": "discipline_model_confidence"}
    present = {k: v for k, v in keep.items() if k in labels.columns}
    labels = labels[list(present)].rename(columns=present)
    if not labels.project_id.is_unique:
        raise SystemExit("duplicate project_id in the discipline labels")
    return projects.merge(labels, on="project_id", how="left")


def add_time_and_funding(frame: pd.DataFrame) -> pd.DataFrame:
    """Duration, elapsed time and funding availability."""
    frame = frame.copy()
    start = pd.to_datetime(frame.start_date, errors="coerce")
    end = pd.to_datetime(frame.end_date, errors="coerce")
    frame["start_year"] = start.dt.year
    frame["end_year"] = end.dt.year
    frame["duration_months"] = ((end - start).dt.days / 30.44).round(1)
    # Time available to produce output: to the project end, or to the
    # reference date for projects still running at that point.
    effective_end = end.where(end < REFERENCE_DATE, REFERENCE_DATE)
    frame["months_elapsed"] = ((effective_end - start).dt.days / 30.44).round(1)
    frame["is_complete"] = (end < REFERENCE_DATE).astype("boolean")

    value = pd.to_numeric(frame.value_gbp, errors="coerce")
    frame["value_gbp"] = value
    # Missing or zero for roughly a fifth of projects, mostly studentships.
    # Use this as the denominator filter for any funding statistic.
    frame["funding_available"] = (value.notna() & (value > 0)).astype("boolean")
    return frame


# ---------------------------------------------------------------------------
# 2. INSTITUTIONS
# ---------------------------------------------------------------------------

def build_institutions() -> tuple[pd.DataFrame, pd.DataFrame]:
    registry = read("institutions")
    if not registry.institution_id.is_unique:
        n = int(registry.institution_id.duplicated().sum())
        warn(f"institution_id is not unique in the registry ({n} duplicate "
             f"rows). Any join to it will fan out. Re-run "
             f"build_institution_registry.py.")
    links = read("project_institutions")
    meta = registry[["institution_id", "id_type", "ror_id", "country_code",
                     "city", "lat", "lon", "type_source"]]
    links = links.merge(meta, on="institution_id", how="left")
    return registry, links


def summarise_institutions(links: pd.DataFrame) -> pd.DataFrame:
    """Per project: the resolved lead organisation, and partner counts."""
    lead = links[links.role == "lead"].rename(columns={
        "institution_id": "lead_institution_id",
        "institution_name": "lead_institution_name",
        "org_type": "lead_org_type", "country_code": "lead_country",
        "city": "lead_city", "lat": "lead_lat", "lon": "lead_lon",
        "ror_id": "lead_ror_id"})
    lead = lead[["project_id", "lead_institution_id", "lead_institution_name",
                 "lead_org_type", "lead_country", "lead_city", "lead_lat",
                 "lead_lon", "lead_ror_id"]]
    if not lead.project_id.is_unique:
        warn(f"{int(lead.project_id.duplicated().sum())} projects have more "
             f"than one lead organisation; keeping the first")
        lead = lead.drop_duplicates("project_id")

    partners = links[links.role == "participant"]
    counts = partners.groupby("project_id").agg(
        n_partner_institutions=("institution_id", "nunique")).reset_index()
    types = partners.groupby(["project_id", "org_type"]).size().unstack(fill_value=0)
    types.columns = [f"n_partners_{c}" for c in types.columns]
    types = types.reset_index()

    out = lead.merge(counts, on="project_id", how="left").merge(
        types, on="project_id", how="left")
    partner_cols = [c for c in out.columns if c.startswith("n_partner")]
    out[partner_cols] = out[partner_cols].fillna(0).astype(int)
    return out


# ---------------------------------------------------------------------------
# 3. OUTCOMES, the output side, from the merged layer
# ---------------------------------------------------------------------------

def build_outcomes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse the link-grain merged file to one row per outcome.

    Returns (outcomes, project_outcomes).
    """
    records = read("merged_outcomes")
    links = read("project_outcome_map")

    if len(records) != len(links):
        raise SystemExit(
            f"merged/outcomes.csv ({len(records)}) and project_outcome_map.csv "
            f"({len(links)}) have different row counts; they are expected to be "
            f"row-for-row aligned")
    if not (records.global_outcome_id.values == links.global_outcome_id.values).all():
        raise SystemExit("merged/outcomes.csv is not row-aligned with "
                         "project_outcome_map.csv on global_outcome_id")

    # Flag outcomes whose type differs between their link rows before
    # collapsing, so the ambiguity is visible rather than resolved silently.
    type_counts = records.groupby("global_outcome_id")["type"].nunique(dropna=False)
    ambiguous = set(type_counts[type_counts > 1].index)
    if ambiguous:
        warn(f"{len(ambiguous)} outcomes carry more than one `type` across "
             f"their link rows (e.g. dataset in GtR, publication in Scopus). "
             f"Flagged as type_ambiguous; the first non-null type is kept.")

    outcomes = (records.groupby("global_outcome_id", as_index=False)
                .first())
    outcomes["n_project_links"] = (links.groupby("global_outcome_id").size()
                                   .reindex(outcomes.global_outcome_id).values)
    outcomes["type_ambiguous"] = outcomes.global_outcome_id.isin(ambiguous)
    outcomes["doi_norm"] = outcomes["doi"].map(normalise_doi)
    outcomes["is_output"] = ~outcomes["type"].isin(MERGED_NOT_OUTPUT)

    # Citation counts. Until 17 August the merge populated `cited_by` from
    # Scopus only, despite advertising a WoS-then-Scopus rule, so a paper held
    # only by WoS read as having no citation count rather than a missing one.
    # That was fixed upstream in the collector, and `cited_by` now carries both
    # sources: coverage went from 4,102 publications to 6,220.
    #
    # The fallback below is therefore a no-op against current data and is kept
    # deliberately, as a guard. If a future collection or merge change
    # reintroduces the gap, `cited_by_best` stays correct and the recovery
    # count prints, rather than the loss passing unnoticed. Use
    # `cited_by_best` in analysis regardless, so nothing has to change if that
    # happens.
    cited = numeric(outcomes, "cited_by")
    wos_cited = numeric(outcomes, "wos_times_cited_all_db")
    outcomes["cited_by_best"] = cited.fillna(wos_cited)
    outcomes["cited_by_source"] = np.where(
        cited.notna(), "merged",
        np.where(wos_cited.notna(), "wos_fallback", None))
    recovered = int((cited.isna() & wos_cited.notna()).sum())
    if recovered:
        print(f"  [WARN] recovered {recovered} citation counts from Web of "
              f"Science that `cited_by` alone would have left null. The "
              f"upstream merge has regressed; tell whoever owns it.")

    # OpenAlex is held out of `cited_by` by deliberate decision, on the
    # grounds that it agrees with Scopus too rarely to merge. It remains
    # available as `openalex_cited_by` for sensitivity analysis.
    oa_only = int((cited.isna()
                   & numeric(outcomes, "openalex_cited_by").notna()).sum())
    if oa_only:
        print(f"  {oa_only} outcomes have an OpenAlex citation count and no "
              f"merged one, excluded by design")

    # Attach the outcome-side discipline where the DOI resolves to one.
    labelled = read("publications_labelled")
    labelled["doi_norm"] = labelled["doi"].map(normalise_doi)
    labelled = (labelled.dropna(subset=["doi_norm"])
                .drop_duplicates("doi_norm")[["doi_norm", "field", "tier"]]
                .rename(columns={"field": "output_field",
                                 "tier": "output_field_tier"}))
    outcomes = outcomes.merge(labelled, on="doi_norm", how="left")

    if not outcomes.global_outcome_id.is_unique:
        raise SystemExit("global_outcome_id is not unique after collapsing")
    check_outcome_schema(outcomes)

    project_outcomes = links.copy()
    return outcomes, project_outcomes


# authors_long.csv's `source` values, mapped to the project_outcomes column
# holding that source's native outcome id.
AUTHOR_NATIVE_ID_COLUMNS = {
    "openalex": "openalex_outcome_id",
    "scopus": "scopus_outcome_id",
    "wos": "wos_outcome_id",
    "gtr": "gtr_outcome_id",
}


def normalise_native_id(value) -> str | None:
    """Native outcome ids as a merge key across tables that store them
    differently: project_outcome_map.csv has passed WoS's UT (a zero-padded
    string like "001794901400001") through a float cast somewhere upstream,
    which drops the leading zeros and appends ".0". Stripping both brings it
    back in line with authors_long.csv's untouched string form; openalex,
    scopus and gtr ids are unaffected by either strip but it's harmless to
    apply it anyway.
    """
    if pd.isna(value):
        return None
    return str(value).removesuffix(".0").lstrip("0") or "0"


def attach_global_outcome_id(outcome_authors: pd.DataFrame,
                             project_outcomes: pd.DataFrame) -> pd.DataFrame:
    """authors_long.csv (-> the `outcome_authors` table) carries `outcome_id`,
    a source-native id, not the deduplicated `global_outcome_id` used
    everywhere else in this database. Without this, joining an author to a
    specific row in `outcomes` means redoing this lookup downstream every
    time. Build it once here instead, from project_outcomes, the only table
    that carries both id systems side by side.

    Rows whose outcome never made it into project_outcomes (i.e. it isn't
    linked to a project in the 1,640-project spine) stay unmatched; that's
    expected, not a bug, and is reported below rather than silently dropped.
    """
    frames = []
    for source, column in AUTHOR_NATIVE_ID_COLUMNS.items():
        sub = project_outcomes.loc[project_outcomes[column].notna(),
                                   ["global_outcome_id", column]]
        sub = sub.drop_duplicates().rename(columns={column: "outcome_id_norm"})
        sub["outcome_id_norm"] = sub["outcome_id_norm"].map(normalise_native_id)
        sub["source"] = source
        frames.append(sub)
    lookup = pd.concat(frames, ignore_index=True).drop_duplicates(
        ["source", "outcome_id_norm"])

    outcome_authors = outcome_authors.copy()
    outcome_authors["outcome_id_norm"] = (
        outcome_authors.outcome_id.map(normalise_native_id))
    merged = outcome_authors.merge(lookup, on=["source", "outcome_id_norm"],
                                   how="left")
    n_missing = int(merged.global_outcome_id.isna().sum())
    if n_missing:
        warn(f"{n_missing} outcome_authors rows "
             f"({100 * n_missing / len(merged):.1f}%) have no "
             f"global_outcome_id; their outcome isn't linked to a project "
             f"in the spine")

    cols = [c for c in merged.columns if c != "outcome_id_norm"]
    cols.insert(cols.index("outcome_id") + 1, cols.pop(cols.index("global_outcome_id")))
    return merged[cols]


# ---------------------------------------------------------------------------
# 4. SOURCE-STACKED COUNTS, the definition the chapter quotes
# ---------------------------------------------------------------------------

def build_outcome_organisations() -> pd.DataFrame:
    """
    The organisations named on GtR collaboration records.

    Gateway to Research populates its `organisations` field for exactly one
    outcome type: collaborations, where it is filled on all 1,621 records and
    empty on all 16,410 others. So this is not author affiliation. It is the
    partner a project reports having collaborated with, which makes it a
    funding-side collaboration network, entirely separate from the
    publication-side co-authorship network built from `outcome_authors`.

    One row per project-outcome-organisation. Names arrive semicolon-joined
    and sometimes carry a trading name alongside a legal one
    ("Constellium; Constellium UK Ltd"), so they are split and stripped but
    not resolved against the institution registry: matching them properly is
    a separate job and doing it badly here would be worse than leaving the
    raw string for the analysis to handle.

    Reporting is a research-council obligation and Innovate UK reports nothing
    to GtR, so coverage is heavily skewed towards education-led projects. That
    is a property of the reporting regime, not of who collaborates, and any
    analysis using this table has to say so.
    """
    gtr = read("gtr_outcomes")
    if "organisations" not in gtr.columns:
        warn("gtr_all_outcomes_clean has no organisations column; "
             "outcome_organisations will be empty")
        return pd.DataFrame(columns=["project_id", "gtr_outcome_id",
                                     "gtr_outcome_type", "organisation"])

    have = gtr[gtr["organisations"].notna()].copy()
    rows = []
    for project_id, outcome_id, otype, value in zip(
            have["project_id"], have["outcome_id"],
            have["gtr_outcome_type"], have["organisations"]):
        for name in str(value).split(";"):
            name = name.strip()
            if name:
                rows.append({"project_id": project_id,
                             "gtr_outcome_id": outcome_id,
                             "gtr_outcome_type": otype,
                             "organisation": name})

    frame = pd.DataFrame(rows).drop_duplicates()
    if len(frame):
        print(f"\nOutcome organisations")
        print(f"  {len(frame):,} project-outcome-organisation rows")
        print(f"  {frame.organisation.nunique():,} distinct organisations "
              f"across {frame.project_id.nunique():,} projects")
        by_type = frame.gtr_outcome_type.value_counts()
        print(f"  outcome types carrying them: {by_type.to_dict()}")
    return frame


def build_source_links(spine_ids: set) -> pd.DataFrame:
    """One row per project-output link, per source, before deduplication.

    This reproduces the counting convention behind the 616 / 1,640 figure in
    the Methodology chapter. It is kept alongside the merged layer rather than
    replaced by it, so the chapter's figure remains reproducible from this
    database.
    """
    rows = []
    gtr = read("gtr_outcomes")
    gtr = gtr[gtr.project_id.isin(spine_ids)]
    rows.append(pd.DataFrame({
        "project_id": gtr.project_id,
        "source": "gtr",
        "outcome_type": gtr.gtr_outcome_type,
        "doi": gtr.get("doi", pd.Series(index=gtr.index, dtype=object)).map(normalise_doi),
        "title": gtr.get("title"),
    }))
    for name, key in [("openalex", "openalex_outcomes"),
                      ("scopus", "scopus_outcomes"),
                      ("wos", "wos_outcomes")]:
        frame = read(key)
        frame = frame[frame.project_id.isin(spine_ids)]
        rows.append(pd.DataFrame({
            "project_id": frame.project_id,
            "source": name,
            "outcome_type": "publications",
            "doi": frame["doi"].map(normalise_doi),
            "title": frame.get("title"),
        }))
    return pd.concat(rows, ignore_index=True)


def summarise_source_links(links: pd.DataFrame, spine_ids: set) -> pd.DataFrame:
    """Per project: a count for every GtR outcome type, plus roll-ups."""
    gtr = links[links.source == "gtr"]
    counts = gtr.groupby(["project_id", "outcome_type"]).size().unstack(fill_value=0)
    for outcome in GTR_OUTCOME_TYPES:
        if outcome not in counts.columns:
            counts[outcome] = 0
    counts = counts[GTR_OUTCOME_TYPES]
    counts.columns = [f"n_{c}" for c in counts.columns]
    counts = counts.reindex(sorted(spine_ids)).fillna(0).astype(int)

    pubs = links[links.outcome_type == "publications"]
    with_doi = pubs[pubs.doi.notna()]
    by_source = with_doi.groupby(["project_id", "source"])["doi"].nunique().unstack(fill_value=0)
    for source in ("gtr", "openalex", "scopus", "wos"):
        if source not in by_source.columns:
            by_source[source] = 0
    by_source = by_source[["gtr", "openalex", "scopus", "wos"]]
    by_source.columns = [f"n_publications_{c}" for c in by_source.columns]

    union_doi = with_doi.groupby("project_id")["doi"].nunique().rename(
        "n_publications_distinct_doi")
    any_record = pubs.groupby("project_id").size().rename("n_publication_records")

    table = (counts.join(by_source).join(union_doi).join(any_record)
             .fillna(0).astype(int).reset_index()
             .rename(columns={"index": "project_id"}))

    table["has_publication"] = table.n_publication_records > 0
    output_cols = [f"n_{t}" for t in OUTPUT_TYPES]
    table["n_outputs_total"] = table[output_cols].sum(axis=1)
    non_pub = [c for c in output_cols if c != "n_publications"]
    table["n_non_publication_outputs"] = table[non_pub].sum(axis=1)
    table["has_non_publication_output"] = table.n_non_publication_outputs > 0
    table["has_any_output"] = table.has_publication | table.has_non_publication_output
    return table


def summarise_merged_outcomes(project_outcomes: pd.DataFrame,
                              outcomes: pd.DataFrame,
                              spine_ids: set) -> pd.DataFrame:
    """Per project: deduplicated outcome counts from the merged layer."""
    typed = project_outcomes.merge(
        outcomes[["global_outcome_id", "type", "is_output", "doi_norm"]],
        on="global_outcome_id", how="left")
    typed = typed[typed.project_id.isin(spine_ids)]

    counts = typed.groupby(["project_id", "type"]).size().unstack(fill_value=0)
    counts.columns = [f"n_merged_{c}" for c in counts.columns]

    totals = typed.groupby("project_id").agg(
        n_outcomes_merged=("global_outcome_id", "nunique")).join(
        typed[typed.is_output == True].groupby("project_id").agg(
            n_outputs_merged=("global_outcome_id", "nunique"))).join(
        typed[typed.type == "publication"].groupby("project_id").agg(
            n_publications_merged=("global_outcome_id", "nunique"))).join(
        typed[typed.doi_norm.notna()].groupby("project_id").agg(
            n_distinct_doi_merged=("doi_norm", "nunique")))

    table = (counts.join(totals).reindex(sorted(spine_ids))
             .fillna(0).astype(int).reset_index()
             .rename(columns={"index": "project_id"}))
    table["has_publication_merged"] = table.n_publications_merged > 0
    table["has_any_output_merged"] = table.n_outputs_merged > 0
    return table


# ---------------------------------------------------------------------------
# 5. WRITE
# ---------------------------------------------------------------------------

VIEWS = {
    # The RQ3 view: everything needed to relate an input to its output, one
    # row per project, no joins required.
    "v_input_output": """
        SELECT p.project_id, p.grant_reference, p.lead_funder, p.grant_category,
               p.value_gbp, p.funding_available, p.start_year, p.end_year,
               p.duration_months, p.months_elapsed, p.is_complete, p.status,
               p.lead_institution_id, p.lead_institution_name, p.lead_org_type,
               p.lead_country, p.n_partner_institutions,
               p.discipline_field, p.discipline_source, p.discipline_confidence,
               p.discipline_tier,
               p.n_publications, p.n_publication_records,
               p.n_publications_distinct_doi, p.has_publication,
               p.n_outputs_total, p.n_non_publication_outputs, p.has_any_output,
               p.n_outcomes_merged, p.n_outputs_merged,
               p.n_publications_merged, p.has_publication_merged,
               p.title_clean
        FROM projects p
    """,
    # Input discipline against output discipline, one row per link that has
    # both. The basis for the RQ3 disciplinary-flow analysis.
    "v_discipline_flow": """
        SELECT p.project_id, p.discipline_field AS project_field,
               p.discipline_tier AS project_field_tier,
               o.global_outcome_id, o.output_field, o.output_field_tier,
               o.type AS outcome_type, o.year AS outcome_year,
               p.lead_org_type, p.lead_funder, p.value_gbp
        FROM project_outcomes po
        JOIN projects p ON p.project_id = po.project_id
        JOIN outcomes o ON o.global_outcome_id = po.global_outcome_id
        WHERE o.output_field IS NOT NULL
    """,
    # Institution-level activity, for RQ1. Counts each project once per
    # institution-role, so a project with five partners contributes to five
    # institutions.
    "v_institution_activity": """
        SELECT i.institution_id, i.canonical_name, i.org_type, i.country_code,
               i.city, i.ror_id,
               COUNT(DISTINCT pi.project_id) AS n_projects,
               SUM(CASE WHEN pi.role = 'lead' THEN 1 ELSE 0 END) AS n_as_lead,
               SUM(CASE WHEN pi.role = 'participant' THEN 1 ELSE 0 END) AS n_as_partner,
               SUM(CASE WHEN pi.role = 'lead' THEN p.value_gbp ELSE 0 END) AS funding_led_gbp,
               SUM(CASE WHEN pi.role = 'lead' THEN p.n_outputs_total ELSE 0 END) AS outputs_led
        FROM project_institutions pi
        JOIN institutions i ON i.institution_id = pi.institution_id
        JOIN projects p ON p.project_id = pi.project_id
        GROUP BY i.institution_id
    """,
    # Every output with the award that funded it. The literal answer to
    # "link all of the inputs to the outputs".
    "v_outcome_detail": """
        SELECT po.project_id, po.global_outcome_id, po.source AS link_sources,
               po.match_basis,
               o.title, o.type, o.subtype, o.year, o.doi, o.source_title,
               o.cited_by, o.cited_by_best, o.cited_by_source,
               o.output_field, o.type_ambiguous,
               p.lead_funder, p.grant_category, p.value_gbp, p.start_year,
               p.lead_institution_name, p.lead_org_type, p.discipline_field
        FROM project_outcomes po
        JOIN outcomes o ON o.global_outcome_id = po.global_outcome_id
        JOIN projects p ON p.project_id = po.project_id
    """,
    # Funding and output by year of award start, for RQ2 time trends.
    "v_yearly": """
        SELECT start_year,
               COUNT(*) AS n_projects,
               SUM(CASE WHEN funding_available = 1 THEN value_gbp ELSE 0 END) AS funding_gbp,
               SUM(n_outputs_total) AS n_outputs,
               SUM(CASE WHEN has_publication = 1 THEN 1 ELSE 0 END) AS n_with_publication
        FROM projects
        WHERE start_year IS NOT NULL
        GROUP BY start_year ORDER BY start_year
    """,
}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_po_project ON project_outcomes(project_id)",
    "CREATE INDEX IF NOT EXISTS ix_po_outcome ON project_outcomes(global_outcome_id)",
    "CREATE INDEX IF NOT EXISTS ix_pi_project ON project_institutions(project_id)",
    "CREATE INDEX IF NOT EXISTS ix_pi_institution ON project_institutions(institution_id)",
    "CREATE INDEX IF NOT EXISTS ix_out_doi ON outcomes(doi_norm)",
    "CREATE INDEX IF NOT EXISTS ix_out_type ON outcomes(type)",
    "CREATE INDEX IF NOT EXISTS ix_sl_project ON source_outcome_links(project_id)",
    "CREATE INDEX IF NOT EXISTS ix_au_outcome ON outcome_authors(outcome_key)",
    "CREATE INDEX IF NOT EXISTS ix_au_identity ON outcome_authors(identity_id)",
    "CREATE INDEX IF NOT EXISTS ix_oo_project ON outcome_organisations(project_id)",
    "CREATE INDEX IF NOT EXISTS ix_oo_org ON outcome_organisations(organisation)",
]


def write_database(tables: dict[str, pd.DataFrame]) -> None:
    # SQLite needs POSIX locking, which some mounted filesystems do not
    # provide; writing straight to a synced folder fails with a disk I/O
    # error. Build in a local temporary directory and copy the finished file
    # across, which is a plain sequential write and always works.
    scratch = Path(tempfile.mkdtemp(prefix="ce_db_")) / DB_PATH.name
    con = sqlite3.connect(scratch)
    try:
        for name, frame in tables.items():
            frame = frame.copy()
            for col in frame.columns:
                if frame[col].dtype == "boolean":
                    frame[col] = frame[col].astype("Int64")
                elif frame[col].dtype == bool:
                    frame[col] = frame[col].astype(int)
            frame.to_sql(name, con, index=False)
            print(f"  {name:28s} {len(frame):7d} rows, {len(frame.columns):3d} cols")
        for statement in INDEXES:
            try:
                con.execute(statement)
            except sqlite3.OperationalError as exc:
                warn(f"index skipped: {exc}")
        for name, sql in VIEWS.items():
            con.execute(f"CREATE VIEW {name} AS {sql}")
            n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  view {name:23s} {n:7d} rows")
        con.commit()
        con.execute("VACUUM")
    finally:
        con.close()
    # Overwrite in place rather than unlink-then-write: some synced folders
    # refuse deletes but allow truncating writes.
    shutil.copyfile(scratch, DB_PATH)
    shutil.rmtree(scratch.parent, ignore_errors=True)


# Columns whose text is too long to be useful in a spreadsheet.
XLSX_DROP = ["abstract_text_clean", "abstract", "abstract_clean", "description",
             "description_clean", "impact", "impact_clean", "name_variants",
             "research_topics", "participant_organisations",
             "openalex_author_names", "openalex_author_ids",
             "openalex_author_orcids", "scopus_author_names",
             "scopus_author_ids", "scopus_author_given_names",
             "wos_researcher_ids", "wos_orcids", "organisations"]

XLSX_TABLES = ["projects", "project_institutions", "institutions", "outcomes",
               "project_outcomes", "project_field_probabilities",
               "publication_field_probabilities", "authors",
               "outcome_authors"]

XLSX_MAX_ROWS = 200_000


def write_workbook(tables: dict[str, pd.DataFrame],
                   contents: pd.DataFrame) -> None:
    with pd.ExcelWriter(XLSX_PATH, engine="openpyxl") as writer:
        contents.to_excel(writer, sheet_name="CONTENTS", index=False)
        for name in XLSX_TABLES:
            if name not in tables:
                continue
            frame = tables[name].drop(
                columns=[c for c in XLSX_DROP if c in tables[name].columns])
            if len(frame) > XLSX_MAX_ROWS:
                warn(f"{name} truncated to {XLSX_MAX_ROWS} rows in the workbook; "
                     f"use the database for the full table")
                frame = frame.head(XLSX_MAX_ROWS)
            # Excel rejects cells over 32,767 characters.
            for col in frame.columns:
                if frame[col].dtype == object:
                    frame[col] = frame[col].astype(str).str.slice(0, 32000)
                    frame[col] = frame[col].replace({"nan": None, "None": None})
            frame.to_excel(writer, sheet_name=name[:31], index=False)
            print(f"  sheet {name:24s} {len(frame):7d} rows, {len(frame.columns)} cols")


def write_manifest(figures: dict) -> None:
    lines = ["# Build manifest", "",
             f"Built {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by "
             f"`scripts/build_final_database.py`.", "",
             f"Reference date for all elapsed-time fields: "
             f"**{REFERENCE_DATE.date()}**.", "",
             "## Inputs", "",
             "| File | Rows | Modified (UTC) | SHA-256 (first 16) |",
             "|---|---:|---|---|"]
    for key, path in INPUT_FILES.items():
        if not path.exists():
            lines.append(f"| `{key}` | MISSING | | |")
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
        mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        with open(path, encoding="utf-8", errors="replace") as handle:
            n = sum(1 for _ in handle) - 1
        rel = path.relative_to(REPO)
        lines.append(f"| `{rel}` | {n:,} | {mtime:%Y-%m-%d %H:%M} | `{digest}` |")

    lines += ["", "## Headline figures", "",
              "| Figure | Value | Chapter | Agrees |", "|---|---:|---:|---|"]
    for name, value in figures.items():
        expected = CANONICAL.get(name)
        if expected is None:
            lines.append(f"| {name} | {value:,} | | |")
        else:
            ok = "yes" if value == expected else "**NO**"
            lines.append(f"| {name} | {value:,} | {expected:,} | {ok} |")

    lines += ["", "## Warnings raised during the build", ""]
    lines += [f"- {w}" for w in WARNINGS] or ["- none"]
    (OUT_DIR / "BUILD_MANIFEST.md").write_text("\n".join(lines) + "\n",
                                               encoding="utf-8")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Repository: {REPO}")
    print(f"Output:     {OUT_DIR}\n")

    print("Inputs")
    projects = build_projects()
    spine_ids = set(projects.project_id)
    print(f"  spine                        {len(projects)} projects")

    projects = add_discipline(projects)
    registry, links = build_institutions()
    print(f"  institution registry         {len(registry)} organisations")
    print(f"  project-institution links    {len(links)}")

    outcomes, project_outcomes = build_outcomes()
    print(f"  merged outcomes              {len(outcomes)} distinct, "
          f"{len(project_outcomes)} project links")

    source_links = build_source_links(spine_ids)
    print(f"  source-stacked output links  {len(source_links)}")

    print("\nAssembling")
    projects = (projects
                .merge(summarise_institutions(links), on="project_id", how="left")
                .merge(summarise_source_links(source_links, spine_ids),
                       on="project_id", how="left")
                .merge(summarise_merged_outcomes(project_outcomes, outcomes,
                                                 spine_ids),
                       on="project_id", how="left"))
    projects = add_time_and_funding(projects)

    if len(projects) != len(spine_ids):
        raise SystemExit(f"row count changed in the merge: {len(spine_ids)} -> "
                         f"{len(projects)}. A join key is duplicated somewhere.")
    if not projects.project_id.is_unique:
        raise SystemExit("duplicate project_id after merge")

    # Referential integrity: every link must point at a row that exists.
    orphan_links = set(project_outcomes.project_id) - spine_ids
    if orphan_links:
        warn(f"{len(orphan_links)} project ids in project_outcome_map are not "
             f"in the 1,640 spine; their links are retained but will not join")
    orphan_inst = set(links.institution_id) - set(registry.institution_id)
    if orphan_inst:
        warn(f"{len(orphan_inst)} institution ids in project_institutions are "
             f"not in the registry")

    tables = {
        "projects": projects,
        "institutions": registry,
        "project_institutions": links,
        "outcomes": outcomes,
        "project_outcomes": project_outcomes,
        "source_outcome_links": source_links,
        "project_field_probabilities": read("discipline_probabilities"),
        "publication_field_probabilities": read("publication_probabilities"),
        "authors": read("author_identities"),
        "outcome_authors": attach_global_outcome_id(read("authors_long"),
                                                     project_outcomes),
        "outcome_organisations": build_outcome_organisations(),
    }

    print("\nCoverage")
    for column, label in [("lead_org_type", "lead organisation type"),
                          ("discipline_field", "discipline label"),
                          ("lead_country", "lead organisation country")]:
        n = int(projects[column].notna().sum())
        print(f"  {label:34s} {n:5d} / {len(projects)} "
              f"({100 * n / len(projects):.1f}%)")

    print("\nOutput, two counting conventions")
    print("  source-stacked (the chapter's definition)")
    for label, series in [("has_publication", projects.has_publication),
                          ("has_non_publication_output",
                           projects.has_non_publication_output),
                          ("has_any_output", projects.has_any_output)]:
        print(f"    {label:32s} {int(series.sum()):5d} "
              f"({100 * series.mean():.1f}%)")
    print("  merged, deduplicated")
    for label, series in [("has_publication_merged",
                           projects.has_publication_merged),
                          ("has_any_output_merged",
                           projects.has_any_output_merged)]:
        print(f"    {label:32s} {int(series.sum()):5d} "
              f"({100 * series.mean():.1f}%)")

    # Text completeness on the output side. Anything downstream that mines
    # titles or abstracts (topic modelling for RQ2) needs to know this before
    # it starts, not after. Reported per build so a regression is visible.
    print("\nOutcome text completeness")
    pubs = outcomes[outcomes["type"] == "publication"]
    for label, frame in [("all outcomes", outcomes), ("publications", pubs)]:
        blank = lambda col: int(frame[col].isna().sum() +
                                (frame[col].astype(str).str.strip() == "").sum()
                                ) if col in frame.columns else -1
        print(f"  {label:14s} n={len(frame):6d}  "
              f"no title {blank('title'):5d}  "
              f"no abstract {blank('abstract'):5d}  "
              f"no DOI {int(frame['doi_norm'].isna().sum()):5d}")
    print("  Missing titles are almost entirely disseminations, "
          "collaborations and further fundings, which GtR records without one.")

    figures = {
        "n_projects": len(projects),
        "n_with_publication": int(projects.has_publication.sum()),
        "n_without_publication": int((~projects.has_publication).sum()),
    }
    print("\nAgainst the chapter")
    for name, value in figures.items():
        expected = CANONICAL.get(name)
        if expected is None:
            print(f"  {name}: {value} (no committed value)")
        elif value == expected:
            print(f"  {name}: {value} matches")
        else:
            warn(f"{name}: recomputed {value}, chapter says {expected} "
                 f"(drift {abs(value - expected)})")

    print("\nDatabase")
    write_database(tables)

    contents = pd.DataFrame([
        {"table": name, "rows": len(frame), "columns": len(frame.columns)}
        for name, frame in tables.items()])
    print("\nWorkbook")
    write_workbook(tables, contents)

    write_manifest(figures)
    print(f"\nWritten to {OUT_DIR}")
    for path in sorted(OUT_DIR.iterdir()):
        print(f"  {path.name:28s} {path.stat().st_size / 1e6:8.1f} MB")
    if WARNINGS:
        print(f"\n{len(WARNINGS)} warning(s); see BUILD_MANIFEST.md")


if __name__ == "__main__":
    sys.exit(main())
