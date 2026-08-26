## Setup

### API keys

This project requires API keys for external services. The `.env` file is excluded from version control using `.gitignore`. Each user should create their own local `.env` file containing their API keys.

1. Create API keys:
* Scopus API - https://dev.elsevier.com
* OpenAlex API - https://developers.openalex.org
* Web of Science Expanded API - https://developer.clarivate.com/apis/wos

2. Create a `.env` file in the project root:

```bash
touch .env
```

3. Add your API keys to `.env`:

```bash
SCOPUS_API_KEY=your_scopus_api_key
OPENALEX_API_KEY=your_openalex_api_key
WOS_API_KEY=your_web_of_science_api_key
```


## Running the Data Pipeline

Run the following scripts in order from the project root directory. Each step generates the data required for the following stage.

### 1. Collect UKRI Gateway to Research (GtR) Projects

Collects the latest UKRI GtR project metadata.

```bash
python3 -m scripts.collection.collect_gtr_projects --outcomes --sectors
```

### 2. Collect UKRI Gateway to Research (GtR) Outcomes

Collects the latest UKRI GtR outcome metadata.

```bash
python3 -m scripts.collection.collect_gtr_outcomes
```

### 3. Clean UKRI Gateway to Research (GtR) Projects

Cleans and standardises UKRI GtR project data.

```bash
python3 -m scripts.cleaning.clean_gtr_projects
```

### 4. Clean UKRI Gateway to Research (GtR) Outcomes

Cleans and standardises UKRI GtR outcome data for downstream analysis and NLP.

```bash
python3 -m scripts.cleaning.clean_gtr_outcomes
```

### 5. Collect OpenAlex Data

Matches UKRI projects to OpenAlex records and retrieves associated project and research output metadata.

```bash
python3 -m scripts.collection.collect_openalex
```

### 6. Clean OpenAlex Projects

Cleans and standardises OpenAlex project metadata.

```bash
python3 -m scripts.cleaning.clean_openalex_projects
```

### 7. Clean OpenAlex Outcomes

Cleans and standardises OpenAlex research output metadata.

```bash
python3 -m scripts.cleaning.clean_openalex_outcomes
```

### 8. Collect Scopus Outcomes

Matches UKRI projects to Scopus records and retrieves associated outcome metadata.

```bash
python3 -m scripts.collection.collect_scopus_outcomes
```

### 9. Clean Scopus Outcomes

Cleans and standardises Scopus outcome metadata.

```bash
python3 -m scripts.cleaning.clean_scopus_outcomes
```

### 10. Collect Web of Science Outcomes

Matches UKRI projects to Web of Science records by grant reference (FG= field
tag) and retrieves outcome metadata, author affiliations and, optionally, cited
references. Requires `WOS_API_KEY` in `.env` and a Web of Science Expanded
subscription. Run `test_wos_api.py` first to confirm the key works and the
grant-matching route is available, and `inspect_wos_record.py` to see the shape
of a single record.

```bash
python3 -m scripts.collection.test_wos_api
python3 -m scripts.collection.collect_wos
```

### 11. Clean Web of Science Outcomes

Cleans and standardises WoS outcome metadata, regenerates the deduplicated
paper-level table from the cleaned rows, and cleans the institution
affiliations.

```bash
python3 -m scripts.cleaning.clean_wos_outcomes
```

### 12. Merge project and outcome datasets

Merges UKRI and OpenAlex projects and outcomes into one dataset each.

```bash
python3 -m scripts.cleaning.merge
```
After completing all steps, cleaned and merged datasets will be available in the `data/cleaned/merged` directory and individual outcome types for OpenAlex including extra metadata are available in 'data/cleaned/outcomes'.

### 13. Build the discipline-classification training corpus

Collects subject-tagged UKRI projects from the wider GtR index, disjoint from the
CE set, and applies the crosswalk to them.

```bash
python3 scripts/classification/collect_gtr_tagged_corpus.py
python3 scripts/classification/apply_crosswalk_to_corpus.py
```

### 14. Embed projects, publications and the corpus

```bash
python3 scripts/classification/embed_texts_mpnet.py
```

`--projects-only` skips publications and corpus. `--no-corpus` re-embeds
projects and publications only, which is what you want after re-collecting
outcomes. Both write the row index alongside each array; a stale index against a
fresh array misaligns every lookup without raising, so downstream scripts refuse
to run when the two disagree.

### 15. Compare methods and training corpora

Roughly five hours in total. Rebuilds the gold set and folds from a named crosswalk,
re-maps the corpus, then runs the method bake-off and set-ups A to H on 25
frozen splits.

```bash
python3 scripts/classification/run_variant.py --crosswalk james
python3 scripts/classification/run_variant.py --crosswalk kirsty
python3 scripts/classification/run_variant.py --crosswalk merged10
python3 scripts/classification/compare_variants.py
```

`james` and `kirsty` are the two independently built crosswalks that
`compare_variants.py` tests against each other. `merged10` is the ten-class
taxonomy used from step 16 onward. Step 18's `test_soft_counts.py` and
`gold_learning_curve.py` both read the `james` folds, so that run is required
even though the final labelling uses `merged10`.

### 16. Set the confidence threshold and label every project

Reads the threshold off the accuracy-reject curve against a target declared in
advance, then labels the 1,341 projects with no funder subject.

```bash
python3 scripts/classification/apply_classifier.py --crosswalk merged10
```

Produces `projects_labelled_final.csv` and `project_field_probabilities.csv`.

### 17. Label the publications

```bash
python3 scripts/classification/label_publications.py
```

Produces `publications_labelled.csv` and
`publication_field_probabilities.csv`.

### 18. Evaluate

```bash
python3 scripts/classification/evaluate_multilabel.py
python3 scripts/classification/score_verification.py
python3 scripts/classification/test_soft_counts.py
python3 scripts/classification/gold_learning_curve.py
python3 scripts/classification/score_intercoder.py --second data/validation/discipline_coding_SECOND_CODER.xlsx
```

Details of each, and the figures they produce, are in
`scripts/classification/README.md`.

---

## Convenience wrappers

The shell scripts in `scripts/` chain the steps above in the right order, with
the reasoning for each in their headers. Run them from the repository root.

| Script | What it does |
|---|---|
| `scripts/run_canonical.sh` | full rebuild from collection through classification |
| `scripts/run_outcomes.sh` | collect and clean all three bibliometric sources |
| `scripts/run_rebuild.sh` | re-embed and re-run the classification comparison |
| `scripts/run_apply.sh` | re-embed, relabel publications, set thresholds and apply |
| `scripts/run_finish_input.sh` | the variant run plus threshold and application |
| `scripts/restore_wos.sh` | restore an archived WoS collection and relabel |

---

## Where the reported figures come from

Every number quoted in the methodology traces to a committed file.

| Figure | File |
|---|---|
| Set-up comparison, A to H | `data/classification/results/setups_summary_merged10.csv` |
| Per-split scores, for the paired tests | `data/classification/results/setups_merged10.csv` |
| Out-of-fold predictions | `data/classification/results/oof_predictions_H.csv` |
| Accuracy-reject curve and threshold | `data/classification/results/accuracy_reject_curve.csv`, `threshold_summary.json` |
| Multi-label evaluation | `data/classification/results/multilabel_evaluation.csv` |
| Learning curve over gold size | `data/classification/results/gold_learning_curve.csv` |
| Blind verification coding | `data/validation/` |

---

## A note on what this pipeline concluded

Single-label discipline assignment to interdisciplinary research proved
unreliable at the per-project level, and this was measured against blind human
coding rather than assumed. The analysis is therefore built from predicted
probability distributions rather than from hard labels, an approach validated at
0.73 percentage points mean absolute error per field against 1.51 for hard
assignment. `scripts/classification/README.md` sets out the evidence.

### 19. Recover author identifiers and standardise author names

Recovers the ORCID and author identifiers the collectors fetched and then
discarded, and renders every author name in one consistent format. Reads the
existing SQLite caches, so there are no API calls, no credentials and nothing
is re-collected. Run `harvest` first.

```bash
python3 -m scripts.enrichment.harvest_author_identifiers
python3 -m scripts.enrichment.standardise_author_names
```

Produces `authors_long.csv` (one row per author position on an outcome),
`author_identities.csv` (one row per identity, with ORCID and observed name
variants) and `authors_standardised.csv`, all in `data/cleaned/authors/`.

Name standardisation is formatting only. It never merges two records or infers
that "Ji, S." and "Ji, Shouxun" are the same person, because leaving one
researcher as two entries is untidy but true, whereas merging two people
publishes a false claim about them.

### 20. Build the institution registry

Resolves every organisation name across GtR, OpenAlex, Scopus and WoS to one
row per real organisation, attaches an organisation type, and re-expresses the
project-organisation relationship in long form. This is what stops "The
University of Manchester" and "UNIVERSITY OF MANCHESTER" being counted as two
institutions.

```bash
python3 -m scripts.enrichment.build_institution_registry --ror
```

The first run resolves around 7,400 organisations against the Research
Organization Registry and takes roughly an hour. Results cache to
`data/cache/ror_lookup.json`, so later runs take seconds and the `--ror` flag
costs nothing. Omitting it builds everything except the ROR identifiers and
needs no network access.

Produces, in `data/cleaned/institutions/`:

| File | Contents |
|---|---|
| `institutions.csv` | one row per organisation: canonical name, type, ROR id, country, city, every observed spelling |
| `institution_name_variants.csv` | each observed surface string mapped to its organisation |
| `project_institutions.csv` | long form, one row per project-organisation pair, carrying the role (lead or participant) |
| `institution_match_report.txt` | match rates by type, and every merge listed so it can be checked by eye |
| `institutions_for_review.csv` | organisations the rules could not resolve, for manual adjudication |

**Use `institutions.csv` and `project_institutions.csv` for any
institution-level counting, not the raw `lead_organisation` string**, which
double-counts.

ROR matches are tiered rather than trusted: accepted automatically above 0.90
where ROR sets its own confidence flag, held for manual review between 0.70 and
0.90, and rejected below. A match to a non-UK organisation is also held for
review, because six UK councils all matched an American nonprofit at
auto-accept confidence. Manual decisions live in
`institution_type_overrides.csv`, which is committed despite the `data/` ignore
rule because it is a hand-authored input rather than a generated output. The
script reads it but never writes it, so re-running cannot destroy the coding.

### 21. Build the final database

The last step of the pipeline. Joins the merged projects and outcomes, the
institution registry, the discipline labels and the author tables into one
relational database, so every analysis starts from the same file rather than
re-deriving the joins for itself.

```bash
python3 -m scripts.cleaning.build_final_database
```

Writes to `data/final/`, which is gitignored like the rest of `data/`. The
database is around 80 MB and is deliberately not committed: the script is what
is shared, and anyone who needs the database builds it.

| File | Contents |
|---|---|
| `ce_ecosystem.db` | SQLite. Ten tables and five views, indexed and keyed |
| `ce_ecosystem.xlsx` | the same tables for browsing, long text columns dropped |
| `BUILD_MANIFEST.md` | every input file with its row count, modification time and SHA-256 |

`project_outcomes` is the join between the input side and the output side, so
linking an award to what it produced needs no further work. `v_input_output`
gives one row per project with funding, duration, organisation type,
discipline and every output count already rolled up.

Column meanings are in `docs/DATA_DICTIONARY.md`. `docs/DATASET_STATUS.md`
records which files are final, which are superseded and which exist only as an
audit trail.

Two counting conventions are carried side by side. `has_publication` uses the
source-stacked definition behind the 616 of 1,640 figure in the methodology,
and `has_publication_merged` uses the deduplicated merged layer, which gives
615. Both are kept so the reported figure stays reproducible.
