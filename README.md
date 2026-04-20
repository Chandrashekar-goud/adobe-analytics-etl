# Adobe Analytics — Search Keyword Performance ETL

> **Business question:** How much revenue is the client getting from external search engines (Google, Bing, Yahoo), and which keywords are performing the best based on revenue?

[![CI/CD — Deploy ETL](https://github.com/Chandrashekar-goud/adobe-analytics-etl/actions/workflows/deploy.yml/badge.svg)](https://github.com/Chandrashekar-goud/adobe-analytics-etl/actions/workflows/deploy.yml)
![Python](https://img.shields.io/badge/python-3.9-blue)
![PySpark](https://img.shields.io/badge/pyspark-3.5.3-orange)
![AWS Glue](https://img.shields.io/badge/AWS-Glue-yellow)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [How the ETL Works](#how-the-etl-works)
- [Attribution Model](#attribution-model)
- [Output](#output)
- [Local Setup](#local-setup)
- [Running Tests](#running-tests)
- [AWS Infrastructure](#aws-infrastructure)
- [CI/CD Pipeline](#cicd-pipeline)
- [Scalability Analysis](#scalability-analysis)
- [Troubleshooting](#troubleshooting)

---

## Overview

This is a production-ready PySpark ETL application that processes Adobe Analytics hit-level data to answer a business question: which external search engine keywords are driving the most revenue?

The pipeline reads a tab-separated file of raw analytics hits, identifies sessions that originated from an external search engine, traces the purchase revenue back to the originating search keyword using a first-touch attribution model, and outputs a ranked tab-delimited report.

**Sample output:**

| Search Engine Domain | Search Keyword | Revenue |
|---|---|---|
| google.com | Ipod | 290.0 |
| bing.com | Zune | 250.0 |
| google.com | ipod | 190.0 |

---

## Architecture

```
┌─────────────────────┐     git push      ┌──────────────────┐     deploy     ┌──────────────────┐
│   Local Development │ ────────────────► │     GitHub        │ ─────────────► │       AWS        │
│                     │                   │                   │                │                  │
│  main.py            │                   │  Source code      │                │  S3 (landing)    │
│  transform.py       │                   │  deploy.yml       │                │  S3 (scripts)    │
│  url_parser.py      │                   │  7 secrets        │                │  AWS Glue        │
│  20 unit tests      │                   │  Actions runner   │                │  S3 (processed)  │
└─────────────────────┘                   └──────────────────┘                └──────────────────┘
```

**Three S3 buckets are used:**

- `analytics-landing-{account}-dev` — raw input files
- `analytics-scripts-{account}-dev` — packaged Python code and entry point
- `analytics-processed-{account}-dev` — ETL output results

---

## Project Structure

```
adobe-analytics-etl/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD pipeline — runs on every push to main
├── config/
│   ├── dev.env                 # Local environment variables
│   └── prod.env                # Production environment variables
├── data/
│   └── data.sql                # Sample hit-level TSV input file
├── infra/
│   └── setup_aws.sh            # One-time AWS infrastructure setup script
├── src/
│   ├── __init__.py
│   ├── main.py                 # CLI entry point — accepts --input and --output args
│   ├── transform.py            # Core ETL business logic (SearchKeywordProcessor class)
│   └── utils/
│       ├── __init__.py
│       ├── logger.py           # Structured logging (stdout → CloudWatch)
│       └── url_parser.py       # Search engine URL parsing + Spark UDFs
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Shared SparkSession fixture
│   ├── test_transform.py       # Integration-style pipeline tests
│   └── test_url_parser.py      # Pure unit tests for URL parsing logic
├── .coveragerc                 # Coverage configuration (excludes entry point)
├── .gitignore
├── pytest.ini                  # Pytest configuration — sets src/ on PYTHONPATH
├── requirements.txt            # Runtime dependencies
└── requirements-dev.txt        # Development and testing dependencies
```

---

## How the ETL Works

The pipeline has six stages executed in sequence inside `SearchKeywordProcessor.run()`:

**Stage 1 — Read and validate input**

Reads the tab-separated hit-level file with an explicit schema to avoid inference overhead. Uses `PERMISSIVE` mode so corrupt rows are logged rather than crashing the job. Validates that all required columns are present before proceeding.

**Stage 2 — Parse search attributes**

Applies two Spark UDFs to the `referrer` column of every row. `extract_search_domain` returns the canonical engine domain (e.g. `google.com`) if the referrer is a known search engine, otherwise `null`. `extract_search_keyword` extracts the search query parameter (`q` for Google/Bing, `p` for Yahoo).

**Stage 3 — Session attribution**

Uses a Spark Window function partitioned by IP address and ordered by `hit_time_gmt`. `first(search_domain, ignorenulls=True)` over an unbounded window propagates the first non-null search domain seen in each session to every hit in that session — including hits that have an internal referrer. This is the first-touch attribution model.

**Stage 4 — Filter purchase events**

Keeps only rows where `event_list` contains the standalone value `1` (purchase) AND the session has a search attribution. Uses a regex `(^|,)\s*1\s*(,|$)` to avoid false matches against events 10, 11, 12, 13, and 14 which all appear in the data.

**Stage 5 — Parse revenue**

The `product_list` column is semicolon-delimited per product and comma-delimited across multiple products. Revenue is the 4th field (index 3) of each product entry. The stage explodes multi-product rows and sums revenue back to hit level.

**Stage 6 — Aggregate and write**

Groups by `(session_search_domain, session_search_keyword)`, sums revenue, sorts descending, and writes a tab-delimited output file named `YYYY-mm-dd_SearchKeywordPerformance.tab`.

---

## Attribution Model

The hardest part of this ETL is that the search keyword and the purchase revenue exist on **different hits within the same session**.

```
Hit 1:  referrer = google.com?q=Ipod   ← search keyword is HERE
Hit 2:  referrer = esshopzilla.com     ← internal navigation
Hit 3:  referrer = esshopzilla.com     ← product view (event 2)
Hit 4:  referrer = esshopzilla.com     ← add to cart (event 12)
Hit 5:  event_list = 1, revenue = $290 ← purchase revenue is HERE
```

A naive approach that only looks at the referrer on the purchase hit would miss the attribution entirely. This pipeline uses a Spark Window function to propagate the search engine context forward across all hits in the session, so that when a purchase is detected the keyword is already available on the same row.

Sessions are defined by IP address. For production use with higher data volumes, a composite session key (IP + user agent, or a proper visitor cookie) would be more accurate.

---

## Output

The output is a tab-delimited file written to the configured output directory. The file is named with the execution date:

```
2026-04-20_SearchKeywordPerformance.tab
```

Contents:

```
Search Engine Domain	Search Keyword	Revenue
google.com	Ipod	290.0
bing.com	Zune	250.0
google.com	ipod	190.0
```

Sorted by Revenue descending so the highest-performing keyword is always at the top.

**Note on keyword case:** `Ipod` and `ipod` appear as separate rows because the raw search queries differ in case. The spec does not call for case normalisation, and this difference may be meaningful (different user intent). This is noted as an open question for the business.

---

## Local Setup

**Prerequisites:** Python 3.9, Java 11 (required — PySpark 3.5.x is incompatible with Java 17/21 locally due to Apache Arrow's `sun.misc.Unsafe` dependency).

```bash
# 1. Clone the repository
git clone https://github.com/Chandrashekar-goud/adobe-analytics-etl.git
cd adobe-analytics-etl

# 2. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements-dev.txt

# 4. Set Java 11 (macOS)
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# 5. Verify Spark starts correctly
python3 -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[1]').getOrCreate(); print('Spark OK:', s.version); s.stop()"
```

**Run locally against the sample data:**

```bash
python3 src/main.py --input data/data.sql --output output/
cat output/$(date +%Y-%m-%d)_SearchKeywordPerformance.tab
```

---

## Running Tests

```bash
# Run all 20 tests with coverage
python3 -m pytest tests/ -v --cov=src

# Run only URL parser unit tests (no Spark required)
python3 -m pytest tests/test_url_parser.py -v

# Run only pipeline integration tests
python3 -m pytest tests/test_transform.py -v
```

**Expected output:**

```
collected 20 items

tests/test_transform.py::TestPurchaseEventFilter::test_only_purchase_events_pass PASSED
tests/test_transform.py::TestPurchaseEventFilter::test_event_10_not_treated_as_purchase PASSED
tests/test_transform.py::TestSessionAttribution::test_first_search_referrer_propagated PASSED
tests/test_transform.py::TestSessionAttribution::test_no_search_session_has_null_attribution PASSED
tests/test_transform.py::TestRevenueAggregation::test_total_revenue_and_order PASSED
tests/test_transform.py::TestRevenueAggregation::test_output_file_created PASSED
tests/test_url_parser.py::TestGetSearchDomain::test_google PASSED
... (14 more)

20 passed in Xs
```

**Test coverage by module:**

| Module | Coverage | Notes |
|---|---|---|
| `logger.py` | 100% | Fully tested via pipeline tests |
| `url_parser.py` | ~85% | Pure Python helpers fully covered; UDF wrappers excluded |
| `transform.py` | ~69% | All six pipeline stages covered |
| `main.py` | 0% | Entry point — excluded from coverage measurement |

---

## AWS Infrastructure

**One-time setup** — run this once before the first deployment:

```bash
chmod +x infra/setup_aws.sh
ENVIRONMENT=dev ./infra/setup_aws.sh
```

This script creates:

- Three S3 buckets with public access blocked and AES256 encryption
- IAM role `GlueETLRole-dev` with least-privilege S3 access
- AWS Glue job `search-keyword-performance-dev` pointing at the scripts bucket

**Manual job trigger** (after initial setup):

```bash
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

RUN_ID=$(aws glue start-job-run \
  --job-name "search-keyword-performance-dev" \
  --arguments "{
    \"--input\": \"s3://analytics-landing-${ACCOUNT}-dev/data/data.sql\",
    \"--output\": \"s3://analytics-processed-${ACCOUNT}-dev/results/\",
    \"--extra-py-files\": \"s3://analytics-scripts-${ACCOUNT}-dev/scripts/src.zip\"
  }" \
  --query 'JobRunId' --output text)

# Poll until complete
while true; do
  STATUS=$(aws glue get-job-run \
    --job-name "search-keyword-performance-dev" \
    --run-id $RUN_ID \
    --query 'JobRun.JobRunState' --output text)
  echo "$(date '+%H:%M:%S') — $STATUS"
  case $STATUS in
    SUCCEEDED) echo "Done"; break ;;
    FAILED|ERROR|TIMEOUT|STOPPED) echo "Failed"; break ;;
  esac
  sleep 30
done
```

---

## CI/CD Pipeline

Every push to `main` triggers `.github/workflows/deploy.yml` automatically:

```
git push to main
      │
      ▼
Run 20 unit tests  ──── fail? deploy blocked
      │
      ▼
Package src/ → src.zip
      │
      ▼
Upload main.py + src.zip to S3 scripts bucket
      │
      ▼
Update Glue job definition (new script version)
      │
      ▼
Trigger Glue job run
      │
      ▼
Poll every 30s until SUCCEEDED or FAILED
```

**Required GitHub repository secrets:**

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM user access key |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret key |
| `AWS_REGION` | e.g. `us-east-1` |
| `S3_LANDING_BUCKET` | `analytics-landing-{account}-dev` |
| `S3_PROCESSED_BUCKET` | `analytics-processed-{account}-dev` |
| `S3_SCRIPTS_BUCKET` | `analytics-scripts-{account}-dev` |
| `GLUE_JOB_NAME` | `search-keyword-performance-dev` |

Set these at: **Settings → Secrets and variables → Actions → New repository secret**

---

## Scalability Analysis

The current implementation is correct and efficient for files of this size. For the 10 GB+ files the team handles in production, the following considerations apply:

**What already scales well:**

- Streaming reads — no in-driver `collect()` on raw data
- Window functions partitioned by IP — avoids full shuffle
- Standard Spark UDFs — no Arrow dependency issues
- Adaptive Query Execution enabled — auto-tunes partition sizes at runtime
- Only the aggregated result (a handful of rows) is ever collected to the driver

**Improvements needed for 10 GB+ files:**

| Concern | Current | Recommendation |
|---|---|---|
| Session key | IP address only | Add `user_agent` to handle shared NAT/office IPs |
| Input partitioning | Full file scan | Partition S3 by date; process one partition at a time |
| Glue worker size | G.1X × 2 | Scale to G.2X × 10 with auto-scaling |
| Output format | Single CSV | Partitioned Parquet; query with AWS Athena |
| UDFs | Standard Python UDFs | Switch to `pandas_udf` on Glue (Java 11) for 10× throughput |

**Recommended production architecture for large files:**

```
S3 (partitioned by date)
  → AWS Glue job (10× G.2X workers, auto-scaling)
  → Adaptive Query Execution enabled
  → Output: partitioned Parquet on S3
  → AWS Athena for ad-hoc analysis
  → CloudWatch alarms on job failures
```

---

## Troubleshooting

**`DirectByteBuffer` crash on local Spark startup**

PySpark 3.4.x is incompatible with Java 17/21. This project uses PySpark 3.5.3 with `--add-opens` JVM flags. Ensure `JAVA_HOME` points to Java 11 for local runs.

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
java -version  # must show 11
```

**`No module named 'utils'`**

The `pytest.ini` file sets `pythonpath = src`. If tests can't find internal modules, confirm `pytest.ini` exists at the project root.

**`SystemExit: 2` on Glue**

AWS Glue injects its own arguments (`--JOB_NAME`, `--TempDir`, etc.) at runtime. The entry point uses `parse_known_args()` to silently ignore these. If you see this error, confirm `main.py` uses `parse_known_args` not `parse_args`.

**`SystemExit: 0` reported as job failure**

AWS Glue's process launcher treats any `sys.exit()` call as an error — including a clean `sys.exit(0)`. The entry point calls `main()` directly without `sys.exit()`.

---

## Supported Search Engines

The following search engines are recognised by the URL parser. Adding a new engine requires a one-line change in `url_parser.py`:

| Engine | Domain | Keyword parameter |
|---|---|---|
| Google | `google.com`, `google.co.uk`, `google.ca` | `q` |
| Bing | `bing.com` | `q` |
| MSN | `msn.com` | `q` |
| Yahoo | `yahoo.com`, `search.yahoo.com` | `p` |
| Ask | `ask.com` | `q` |
| AOL | `aol.com` | `q` |
| DuckDuckGo | `duckduckgo.com` | `q` |