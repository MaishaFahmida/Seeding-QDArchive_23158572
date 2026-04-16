# QDArchive Seeding Project

**Student:** Maisha Fahmida

**Student ID:** 23158572

**University:** FAU Erlangen-Nürnberg

**Supervisor:** Prof. Dr. Dirk Riehle

---

## Project Context

This project contributes to the **QDArchive (Qualitative Data Archive)** by developing an automated pipeline that:

* identifies qualitative datasets from research repositories
* retrieves available project files
* extracts and stores metadata in a structured SQLite database

The focus is on datasets compatible with **Qualitative Data Analysis (QDA)** tools such as:

* NVivo
* ATLAS.ti
* MAXQDA
* REFI-QDA (.qdpx)

---

## Project Goals

* Automate dataset discovery across repositories
* Retrieve **accessible project files**
* Store structured metadata in SQLite
* Preserve **original license information**
* Prepare data for **further analysis (Part 2)**

---

## Data Sources Used

| Repository                 | ID | Approach                           |
| -------------------------- | -- | ---------------------------------- |
| **AUSSDA** (Dataverse)     | 1  | API-based + direct file download   |
| **UK Data Service (UKDS)** | 2  | DataCite API + metadata extraction |

---

## System Workflow

```
main.py
   ↓
AUSSDA pipeline        UKDS pipeline
   ↓                      ↓
File download         Metadata extraction
   ↓                      ↓
SQLite Database (5 tables)
```

---

## Database Design

The system uses a **normalized SQLite schema** with five interconnected tables:

| Table         | Purpose                          |
| ------------- | -------------------------------- |
| `projects`    | Stores dataset-level information |
| `files`       | Tracks file download results     |
| `keywords`    | Stores associated keywords       |
| `person_role` | Captures authors and roles       |
| `licenses`    | Stores licensing information     |

---

## File Processing Status

| Status          | Description                  |
| --------------- | ---------------------------- |
| `SUCCEEDED`     | File successfully downloaded |
| `FAILED_SERVER` | Download attempt failed      |

---

## Repository Processing Logic

### AUSSDA (Primary Data Source)

* Accessed via Dataverse API
* Downloads **individual dataset files**
* Extracts:

  * title
  * description
  * authors
  * keywords
  * license
* Stores both **files and metadata**

Serves as the **main source of downloadable data**

---

### UK Data Service (Metadata Extraction Only)

* Accessed via **DataCite API**
* Extracts:

  * DOI
  * title
  * description
  * authors
  * keywords
* No file downloads performed

#### Explanation

* Most UKDS datasets:

  * require authentication
  * require user agreements
  * do not provide direct download links

 Therefore, only metadata is collected

---

## Execution Guide

### Install Required Libraries

```bash
pip install requests beautifulsoup4
```

---

### Run the Pipeline

```bash
python main.py
```

---

### Generated Outputs

**Database:**

```
23158572_id-seeding.db
```

**Downloaded files:**

```
data/downloads/aussda/
```

---

## Folder Organization

```
QDA_Maisha/
│
├── main.py
├── 23158572_id-seeding.db
│
├── database/
│   ├── __init__.py
│   └── database.py
│
├── repositories/
│   ├── __init__.py
│   ├── aussda_repository.py
│   └── ukds_repository.py
│
├── data/
│   └── downloads/
│       ├── aussda/
│       └── ukds/
```

---

## Data Acquisition Approach

### AUSSDA

* Query API → retrieve dataset metadata
* Access dataset endpoint → get file list
* Download files using file IDs

### UKDS

* Query DataCite API → discover datasets
* Resolve DOI → access landing page
* Extract metadata only

---

## Example Output Summary

| Repository | Projects     | Files            |
| ---------- | ------------ | ---------------- |
| AUSSDA     | ✔ Downloaded | ✔ Downloaded     |
| UKDS       | ✔ Metadata   | ❌ Not downloaded |

---

## Known Constraints

### Restricted Access (UKDS)

* Many datasets:

  * require login
  * require agreements
* No consistent public download mechanism

---

### Missing License Information

* Some datasets lack license metadata via API
* Current handling:

  * store raw license if available
  * skip otherwise (UKDS)

---

### File Variability

* Some files:

  * are not QDA-related
  * are too large or unsupported

---

### Duplicate Entries

* Same dataset may appear multiple times
* No duplicate filtering implemented yet

---

## Design Choices

* Prioritized **AUSSDA for full data extraction**
* Used **DataCite API for UKDS discovery**
* Preserved **original license values**
* Allowed **metadata-only entries for restricted data**

---

## Possible Enhancements

* Implement duplicate detection using DOI
* Improve license standardization
* Extend support to more repositories
* Enhance QDA file detection
* Build analysis pipeline (Part 2)

---

## License Handling Strategy

The system stores the **license exactly as provided by the source**.

> “If there is a different original data string identifying the license, use this and fix later.”

---

## References & Credits

* QDArchive Project
* REFI-QDA standard
* AUSSDA (Dataverse)
* UK Data Service
* DataCite API

---

## Project Outcome

This project successfully delivers a **functional data seeding pipeline**:

* ✔ Retrieves real qualitative datasets (AUSSDA)
* ✔ Collects large-scale metadata (UKDS)
* ✔ Organizes data in a structured database

It fulfills key requirements of:

* data collection
* metadata extraction
* database design
* pipeline automation

---
