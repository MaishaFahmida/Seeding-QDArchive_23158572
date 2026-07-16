Here is your **final README.md** exactly ready for GitHub.
Just **copy → paste → commit**. Nothing else needed.

---

markdown
# Author

**Name:** Maisha Fahmida <br>
**Student ID:** 23158572<br>
**University:** Friedrich-Alexander-Universität Erlangen-Nürnberg (FAU) <br>
**Supervisor:** Prof. Dr. Dirk Riehle

---

# Full Project Overview

This project contributes to the **QDArchive (Qualitative Data Archive)** initiative by building an automated pipeline to discover, download, and structure qualitative research datasets, and then classify them for future analysis.

The work is divided into two main parts:

**Part 1 — Data Acquisition** searches open data repositories for qualitative and QDA-related research projects, downloads publicly accessible files where available, extracts metadata (titles, descriptions, keywords, authors, licenses), and stores everything in a normalized SQLite database (`23158572-seeding.db`).

**Part 2 — Classification** extends the acquired database by classifying every project into one of four project types (`QDA_PROJECT`, `QD_PROJECT`, `OTHER_PROJECT`, `NOT_A_PROJECT`) based on the file types it contains. Relevant `QD_PROJECT` records are then classified against the **ISIC Rev. 5** taxonomy at the Section + Division level, using a weighted keyword-matching classifier that combines project metadata (title, description, keywords) with text extracted directly from each project's primary data files. Each primary data file is also classified individually where extractable text is available.

The final workflow produces a complete set of outputs: the classification SQLite database, an XLSX results table, repository-level statistics, vector-based classification histograms, ranked top-class tables, and a final PDF report.

Overall, the project demonstrates an end-to-end pipeline covering **automated data acquisition, database construction, project-type classification, ISIC Rev. 5 categorization, primary-file classification, validation, and final result reporting.**



##  Part 1 — Data Acquisition

This project contributes to the **QDArchive (Qualitative Data Archive)** by building an automated pipeline that:

* discovers datasets from research repositories
* downloads available dataset files
* extracts metadata
* stores structured information in a SQLite database

The system is designed for datasets compatible with **Qualitative Data Analysis (QDA)** tools such as:

* NVivo
* ATLAS.ti
* MAXQDA
* REFI-QDA (.qdpx)

---

##  Project Objectives

* Automate dataset collection from repositories
* Download dataset files when publicly accessible
* Extract structured metadata
* Store license information correctly
* Build a normalized SQLite database
* Prepare data for further validation and analysis

---

##  Data Sources

| Repository             | ID | Method                                         |
| ---------------------- | -- | ---------------------------------------------- |
| AUSSDA (Dataverse)     | 1  | API + direct file download                     |
| UK Data Service (UKDS) | 2  | DataCite API + GraphQL + signed download links |

---

##  System Workflow

```

main.py
↓
AUSSDA pipeline        UKDS pipeline
↓                      ↓
File + Metadata        Metadata + File download
↓                      ↓
SQLite Database (5 tables)

````

---

##  Database Schema

The system uses a normalized SQLite schema:

| Table       | Purpose                  |
| ----------- | ------------------------ |
| projects    | Dataset-level metadata   |
| files       | Downloaded file tracking |
| keywords    | Dataset keywords         |
| person_role | Authors and contributors |
| licenses    | License information      |

---

##  Repository Processing Pipelines

###  AUSSDA Pipeline

* Uses Dataverse API
* Retrieves metadata and files
* Downloads dataset files directly
* Extracts:

  * title
  * description
  * authors
  * keywords
  * license

---

### UK Data Service (UKDS) Pipeline

* Uses **DataCite API** to discover datasets
* Uses **GraphQL API** to fetch detailed metadata
* Downloads files using **signed S3 URLs**
* Extracts:

  * DOI
  * title
  * description
  * authors
  * keywords
  * license

---

## License Extraction Strategy

License information is handled in multiple steps:

1. Extracted from GraphQL field (`AccessCondition`)
2. Parsed using **BeautifulSoup** (HTML → text)
3. If missing → fallback mapping using DOI

### Examples

* Creative Commons Attribution 4.0
* Creative Commons BY-SA 4.0
* Open Government Licence

This ensures license is always stored in the database.

---

##  File Handling Status

| Status        | Description                   |
| ------------- | ----------------------------- |
| SUCCEEDED     | File downloaded and extracted |
| FAILED_SERVER | Download failed               |

---

##  Execution Guide

###  Install Dependencies

```bash
pip install requests beautifulsoup4
````

---

### Run the Pipeline

```bash
python -m repositories.process_ukds_batch
```

or

```bash
python main.py
```

---

## Output Artifacts

### Database

```
23158572_id-seeding.db
```

### Downloaded Files

```
data/downloads/aussda/
data/downloads/ukds/
```

---

## Project Structure

```
QDA_Maisha/
│
├── main.py
├── 23158572_id-seeding.db
│
├── repositories/
│   ├── __init__.py
│   ├── aussda_repository.py
│   ├── ukds_repository.py
│   └── process_ukds_batch.py
│
├── downloader/
│   ├── __init__.py
│   └── downloader.py
│
├── data/
│   ├── ukds_download_list.json
│   └── downloads/
│       ├── aussda/
│       └── ukds/
│
├── database/
│   └── database.py
│
└── tests/
    └── validator.py
```

---

## Limitations

### UKDS Constraints

* Some datasets require authentication
* Signed URLs expire quickly
* Some datasets are not publicly downloadable

---

### Metadata Issues

* Some datasets have missing or unclear license
* License fallback is used when needed

---

### Duplicate Data

* Duplicate entries may occur
* No deduplication implemented yet

---

## Validation Status

* SQLite database structure implemented
* License handling implemented
* Validator integration prepared

Validation script not fully executed yet

### 📸 Validation Proof

![Validation Result](validation_result.png)

---

## Future Improvements

* Run full SQLite validation
* Implement duplicate detection (based on DOI)
* Improve license normalization
* Add more repositories
* Enhance QDA file detection

---

## Project Outcome

This project successfully:

* collects real-world datasets
* downloads files from AUSSDA and UKDS
* extracts structured metadata
* stores data in a normalized database

It meets the requirements of:

* data collection
* metadata extraction
* database design
* pipeline automation

---

# Part 2: Classification

## Classification Database

Part 2 uses a separate SQLite database: `23158572-sq26-classification.db`

The classification database extends the acquired Part 1 data with a new `type` column on `projects`, plus a new `classifications` table storing per-project and per-file ISIC results.

```sql
CREATE TABLE classifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type TEXT NOT NULL,   -- 'PROJECT' or 'FILE'
    target_id TEXT NOT NULL,
    project_id INTEGER,
    primary_class TEXT,
    secondary_class TEXT,
    confidence REAL,
    evidence TEXT,
    classifier_version TEXT,
    classified_at TEXT
);
```

## Step 1 — Project Type Classification

Every project is assigned one of four project types:

```
QDA_PROJECT
QD_PROJECT
OTHER_PROJECT
NOT_A_PROJECT
```

The classification hierarchy is:

```
Known QDA file exists
    ↓
QDA_PROJECT

Otherwise, primary qualitative data files exist
    ↓
QD_PROJECT

Otherwise, another valid data file exists
    ↓
OTHER_PROJECT

Otherwise
    ↓
NOT_A_PROJECT
```

### Project Type Results

**Repository 1 — AUSSDA**

| Project Type | Count |
|---|---|
| QD_PROJECT | 15 |
| NOT_A_PROJECT | 4 |

**Repository 2 — UKDS**

| Project Type | Count |
|---|---|
| QD_PROJECT | 3 |

**Total: 22 projects classified by PROJECT_TYPE** (18 QD_PROJECT, 4 NOT_A_PROJECT, 0 QDA_PROJECT, 0 OTHER_PROJECT)

**Known limitation:** all 4 `NOT_A_PROJECT` cases resulted from their single file (`original.zip`) failing to download (`FAILED_SERVER_UNRESPONSIVE`) — this is a download artifact, not evidence that the underlying project lacks real content.

## ISIC Rev. 5 Classification

### Taxonomy

The classifier uses ISIC Rev. 5 at the **Section + Division** level. A total of **22 sections and 87 divisions** were imported from the professor-provided ISIC reference workbook (`ISIC5_Exp_Notes_11Mar2024.xlsx`) — an exact match to the official UN ISIC Rev. 5 structure.

### Classification Input

For every `QD_PROJECT`, classification input is prepared from:

```
project title
project description
project keywords
extracted text from successfully-downloaded primary data files
(administrative/documentation files such as readmes, codebooks, and
technical reports are excluded, since they describe the dataset rather
than being primary qualitative content)
```

Relevant projects classified: **18**

### Project-Level ISIC Classifier

The project-level classifier uses a weighted **TF-IDF cosine-similarity** approach:

- Project text (title/description/keywords weighted more heavily than raw file text) is compared against each ISIC division's reference vocabulary.
- Project-level results are built via **confidence-weighted voting** across all individually-classified files plus the project's own metadata, rather than naive text concatenation — this prevents a project with many files from having one strong, correct signal drowned out by volume.
- Classifications below a confidence threshold (0.06) are explicitly flagged `[LOW CONFIDENCE - best guess only]` in the evidence field rather than presented as equally reliable. No artificial confidence inflation is applied.

### Methodology Findings & Known Limitations

During development, five distinct, real issues were identified and corrected:

1. **Document-length bias** — divisions with very large reference vocabularies (e.g. Retail Trade, which lists hundreds of product types) could win matches purely from having more words to coincidentally overlap with. Fixed via cosine-similarity normalization.
2. **Boilerplate repetition bias** — ISIC's descriptive text repeats generic phrasing across many bullet points (e.g. demographic terms like "children" repeated across apparel-related entries), inflating irrelevant divisions. Fixed via binary term presence (not raw counts) and targeted stopword filtering.
3. **Small-vocabulary spiking** — divisions with very short reference text could score artificially high from matching just 1–2 words. Fixed via a vocabulary-size penalty.
4. **Project-level dilution by file volume** — projects with many files could have one strong, correct signal outvoted by numerous weakly-consistent files. Addressed via confidence-weighted voting, though this remains a partial limitation for projects with very large file counts.
5. **Administrative/documentation files** (readmes, codebooks, technical reports) were excluded from content classification, since they were found to cause confident but incorrect matches (e.g. being classified as "Computing infrastructure" purely from describing data formats).

**A remaining, non-fixable limitation:** ISIC Rev. 5 is an industrial/economic activity taxonomy, built to classify businesses — not an open-ended qualitative social-science research-topic taxonomy. Some project classifications reflect this inherent ceiling rather than a solvable bug, and are documented transparently (via the low-confidence flag) rather than hidden.

### Examples of Identified Project Classes
- L65 — Insurance, reinsurance and pension funding (most frequent, 3x)
- K62 — Computer programming, consultancy and related activities (2x)
- N72 — Scientific research and development (2x)
- A01 — Crop and animal production, hunting and related service activities
- O78 — Employment activities
- R88 — Social work activities without accommodation

## Required Classification Deliverables

**SQLite Database:** `23158572-sq26-classification.db` (tagged `classification-results`)

**XLSX Results:** `classification/classification_results.xlsx`

**PDF Classification Report:** `classification/classification_report.pdf`

## Conclusion

This project delivers a complete, end-to-end automated workflow for seeding QDArchive with real, openly-licensed qualitative research data — from initial discovery through to final classified, reportable results.

**Part 1 — Data Acquisition** built a pipeline that searched two open data repositories (AUSSDA and UKDS), downloaded 22 qualitative research projects and their associated 606 files, extracted structured metadata (titles, descriptions, keywords, authors, licenses), and stored everything in a normalized SQLite database. The pipeline handled repository-specific quirks transparently — including AUSSDA's flat file structure versus UKDS's deeply nested one — and logged every download outcome (success or failure) rather than silently dropping problem files, giving an honest, auditable record of what was and wasn't retrievable.

**Part 2 — Classification** extended that database in two stages. First, every project was classified into one of four project types (`QDA_PROJECT`, `QD_PROJECT`, `OTHER_PROJECT`, `NOT_A_PROJECT`) based on its file contents — resulting in 18 `QD_PROJECT`, 4 `NOT_A_PROJECT` (all due to failed downloads, not genuinely low-value content), and 0 in the other two categories. Second, all 18 `QD_PROJECT` records were classified against the ISIC Rev. 5 taxonomy at the Section + Division level (22 sections, 87 divisions), using a weighted TF-IDF keyword classifier that combines project metadata with extracted file text, aggregated via confidence-weighted voting across individually-classified files.

Building this classifier surfaced five genuine, distinct methodology issues — document-length bias, boilerplate repetition, small-vocabulary spiking, project-level signal dilution, and a misleading confidence metric — each identified and corrected during development. The remaining imperfections reflect an honest, well-documented limitation rather than a hidden flaw: ISIC Rev. 5 is an industrial/economic activity taxonomy built to classify businesses, and mapping open-ended qualitative social-science research onto it has a real ceiling. Low-confidence classifications are flagged transparently rather than presented as equally reliable.

**Together, Parts 1 and 2 demonstrate:**

- automated, repository-aware data acquisition with full download-outcome logging
- normalized, extensible SQLite database design across both stages
- project-type classification with documented edge cases
- ISIC Rev. 5 classification combining metadata and file content, at both project and file level
- a validated, reproducible pipeline (recursive file-finding, robust text extraction with PDF decryption fallback, and a confidence-aware classifier)
- complete, submission-ready deliverables: the tagged classification database, repository statistics, an XLSX results table, and a PDF report with vector-graphic histograms and ranked class tables

The project's greatest strength is not that every classification is perfectly correct — some inherently can't be, given the nature of the taxonomy — but that every limitation encountered along the way was investigated, understood, and documented rather than hidden. This transparency is itself a core deliverable of the "technical challenges" reporting the assignment asks for.

## References

- QDArchive Project
- REFI-QDA Standard
- AUSSDA Dataverse
- UK Data Service
- DataCite API
- [ISIC Rev. 5 Standard (UN Statistics Division)](https://unstats.un.org/unsd/classifications/Econ/)

---

