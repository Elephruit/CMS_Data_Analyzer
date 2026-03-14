# CLI Coder Prompt: Rust CMS Monthly Enrollment Hyper-Efficient Store

## Role

You are building a production-grade Rust application that downloads user-selected monthly CMS Medicare Advantage / Part D contract-plan-state-county enrollment files, normalizes them, and stores them in a hyper-efficient analytical format optimized for future web app analysis.

The output of this project is a Rust CLI application first. It must be modular, testable, incremental, and designed so a future local web app or API layer can sit on top of the stored data with extremely fast read performance.

---

## Product Objective

Build a Rust-based CLI tool that:

1. Discovers and downloads one or more user-selected monthly CMS files from the CMS Monthly Enrollment by Contract/Plan/State/County area.
2. Parses the ZIP contents defensively, supporting variation in file names and internal layout.
3. Joins contract and enrollment content by contract ID + plan ID where required.
4. Drops all rows where enrollment is `*`.
5. Avoids storing repeated descriptive metadata over and over.
6. Stores plan metadata once and reuses it across months.
7. Stores county metadata once and reuses it across months.
8. Stores monthly enrollment as compact time series keyed by plan and county.
9. Supports incremental ingestion: when a new month is added, only the changed structures should be updated.
10. Produces a storage layout that is compact on disk and extremely fast for future analysis in a web app.

This is not a raw archival mirror of CMS files. It is a transformed analytical store.

---

## Core Design Principle

Do **not** persist the CMS source layout as the primary stored truth.

The CMS layout is row-heavy and repeats plan names, counties, and related descriptive fields across many rows and months. That is wasteful.

Instead, ingest the source, normalize it, deduplicate repeated metadata, and persist a compact analytical representation.

### Guiding concept

Do not store this repeatedly:

* `Part B Credit PPO, Jackson County, January, 123`
* `Part B Credit PPO, Jackson County, February, 143`
* `Part B Credit PPO, Jackson County, March, 124`
* `Part B Credit PPO, Carlton County, January, 583`
* `Part B Credit PPO, Carlton County, February, 382`

Instead store:

* one plan record for `Part B Credit PPO`
* one county record for `Jackson County`
* one county record for `Carlton County`
* one compact monthly series for `(plan, Jackson County)`
* one compact monthly series for `(plan, Carlton County)`

---

## Required Architecture

Implement the project in clearly separated layers.

### 1. Discovery / Fetch Layer

Responsibilities:

* Accept one or more months from the user in `YYYY-MM` format.
* Discover the correct CMS page and ZIP link for each requested month.
* Download the ZIP file.
* Compute a SHA-256 hash of the source ZIP.
* Avoid re-downloading a month if already ingested unless a force flag is provided.

### 2. Parse / Normalize Layer

Responsibilities:

* Unzip the source.
* Detect which internal file or files contain the required data.
* Support either:

  * a single file containing all needed values, or
  * separate files that need to be joined.
* Normalize column names and values.
* Normalize string whitespace and casing where appropriate.
* Parse enrollment safely.
* Skip rows where enrollment is `*`.
* Track skip counts and reasons.

### 3. Key Resolution / Deduplication Layer

Responsibilities:

* Resolve a stable natural key for a plan using `contract_id + plan_id`.
* Create or reuse a compact integer `plan_key`.
* Create or reuse a compact integer `county_key`.
* Version plan metadata only when descriptive attributes materially change.
* Maintain dimension tables for plans, counties, and months.

### 4. Analytical Storage Layer

Responsibilities:

* Persist a compact analytical format, not a giant exploded table.
* Store descriptive metadata in dimension tables.
* Store monthly enrollment in compact per-plan-per-county series.
* Support incremental updates when new months arrive.
* Persist source manifests and validation summaries.

### 5. Query / Serving Layer

Responsibilities:

* Expose a Rust query module usable by a future local web app or API.
* Support very fast reads for:

  * one plan across many months
  * one county across many plans
  * state rollups
  * top gainers / decliners
  * trend charts

---

## Storage Strategy

Use a hybrid design.

### Source of truth

Use **Parquet** for persisted analytical storage.

Reason:

* compact columnar storage
* good compression
* easy interoperability with DuckDB and future analytics tooling
* easy partitioning
* future-proof for export and query

### Optional high-speed serving cache

Also support an optional Rust-native compressed binary cache for ultra-fast reads in a future app.

Do not make the binary cache the only source of truth.

---

## Required Data Model

Create the following logical structures.

### `plan_dim`

Stores plan metadata once.

Suggested fields:

* `plan_key: u32`
* `contract_id: String`
* `plan_id: String`
* `plan_natural_key: String` where format is `CONTRACT|PLAN`
* `plan_name: String`
* optional descriptive metadata found in source files
* `name_hash: u64` or similar
* `valid_from_month: u32`
* `valid_to_month: Option<u32>`
* `is_current: bool`

Rules:

* natural identity is `contract_id + plan_id`
* if the descriptive fields match an existing current record, reuse `plan_key`
* if they materially differ, create a new versioned record and close out the old validity window

### `county_dim`

Stores county metadata once.

Suggested fields:

* `county_key: u32`
* `state_code: String`
* `county_name: String`
* optional normalized county identifier if available or derivable
* `county_natural_key: String` such as `STATE|COUNTY`
* `geo_hash: u64`

Rules:

* county identity should not rely on raw text alone without normalization
* normalize whitespace and canonical naming before keying

### `month_dim`

Suggested fields:

* `month_key: u32`
* `year: i32`
* `month: u8`
* `yyyymm: u32`

Rules:

* month keys should be stable and deterministic
* use a simple sortable representation such as `202603`

### `plan_county_series`

This is the primary fact structure.

Suggested fields:

* `plan_key: u32`
* `county_key: u32`
* `start_month_key: u32`
* `month_count: u16`
* `presence_bitmap: Binary` or compact equivalent
* `enrollment_blob: Binary`
* optional metadata such as min/max month present

Meaning:

* there should be one logical record per `plan_key + county_key`
* enrollment values for multiple months should be compacted into a single series structure, not stored as one giant row-per-month dataset in the primary persisted form

---

## Series Encoding Strategy

Use a practical but efficient encoding.

### V1 recommendation

For the first implementation:

* maintain month order based on `month_dim`
* use a presence bitmap to identify which months exist
* store numeric enrollments in a compact ordered vector
* compress the vector using Zstandard or via Parquet compression

This is preferred over prematurely implementing an overly clever codec.

### V2 enhancement options

After the first version works, consider:

* base + delta encoding
* varint encoding
* bitpacking
* segmented series storage by year

Do not over-optimize before the base pipeline is correct.

---

## Partitioning Strategy

Partition persisted fact storage by:

* `year`
* optionally `state`

Recommended on-disk layout:

```text
store/
  manifests/
    months.json
    ingestion_log.json
  dims/
    plan_dim.parquet
    county_dim.parquet
    month_dim.parquet
  facts/
    year=2025/
      state=MI/
        plan_county_series.parquet
      state=MN/
        plan_county_series.parquet
    year=2026/
      state=MI/
        plan_county_series.parquet
```

Optional cache layout:

```text
cache/
  plan_lookup.bin
  county_lookup.bin
  series_index.bin
  series_values.bin
```

---

## What the CLI Must Do

Implement a CLI application with commands similar to these.

```bash
ma_store fetch-month --month 2026-03
ma_store fetch-range --from 2025-01 --to 2025-12
ma_store list-months
ma_store validate-store
ma_store rebuild-cache
ma_store query plan-trend --contract H1234 --plan 001 --state MI --county "Jackson"
ma_store query county-snapshot --state MI --county "Jackson" --month 2026-03
```

The exact names can vary, but the behavior should cover these use cases.

---

## Required Functional Requirements

### Month ingestion

1. User provides one month or a range of months.
2. App validates month format.
3. App discovers the CMS page for that month.
4. App finds the ZIP link.
5. App downloads the ZIP.
6. App hashes the ZIP.
7. App unpacks and parses it.
8. App joins enrollment and contract content if necessary.
9. App skips any row where enrollment is `*`.
10. App normalizes and resolves plan and county keys.
11. App merges the month into the analytical store.
12. App updates manifests and validation logs.

### Incremental update behavior

When a month is added:

* do not rebuild the full store unless necessary
* update only touched dimensions and touched series
* update month manifest
* avoid duplicate ingestion for the same month unless a replace or force option is used

### Query support

Implement a query layer that supports:

* one plan over time for one or more counties
* one county snapshot for one month
* state totals over time
* plan comparison within a county
* top movers between months

---

## Required Nonfunctional Requirements

1. Code must be modular.
2. Code must be testable.
3. Ingestion should be streaming-oriented where practical.
4. Avoid materializing the full raw join into memory if possible.
5. The store must be compact on disk.
6. The store must be optimized for fast read access by a future app.
7. Each ingest run must output a validation summary.
8. Logging must be structured and useful.
9. Error messages must be explicit and actionable.

---

## Important Business Logic

### 1. Drop starred enrollment rows

Rows where enrollment is `*` must not be stored in the analytical layer.

Track how many were skipped.

### 2. Repeated plan names must not be stored repeatedly

If the same plan metadata appears month after month, store it once in `plan_dim` and reference it via `plan_key`.

### 3. Plan names generally stay stable, but code for exceptions

Do not assume names will never change.

Use this logic:

* plan natural key is `contract_id + plan_id`
* if descriptive metadata matches current version, reuse current `plan_key`
* if descriptive metadata changes materially, version the dimension record

### 4. County metadata must also be deduplicated

Do not store state and county strings in every month record.

### 5. Primary persisted fact shape should be compact series, not exploded monthly rows

Any temporary monthly row representation should be transient only.

---

## Implementation Guidance

### Recommended Rust crates

Use crates in this spirit. Equivalent alternatives are acceptable.

* `reqwest` for HTTP
* `tokio` for async orchestration
* `scraper` for HTML parsing
* `zip` for ZIP handling
* `csv` for CSV parsing
* `serde` and `serde_json` for manifests and config
* `chrono` for date/month logic
* `clap` for CLI parsing
* `thiserror` and/or `anyhow` for error handling
* `hashbrown` or fast hash maps where helpful
* `rayon` for CPU-parallel transforms when useful
* `parquet` and `arrow` crates or `polars` for Parquet persistence
* `zstd` for cache compression

### Project layout

Use a structure similar to:

```text
src/
  main.rs
  cli.rs
  config.rs
  cms/
    discover.rs
    download.rs
    parse.rs
  ingest/
    normalize.rs
    resolver.rs
    merge.rs
    validate.rs
  storage/
    parquet_store.rs
    binary_cache.rs
    manifests.rs
  query/
    read_api.rs
    aggregates.rs
  model/
    plan.rs
    county.rs
    month.rs
    series.rs
  util/
    hashing.rs
    io.rs
    logging.rs
```

---

## Discovery Requirements

The app must not rely on one fragile hardcoded file name.

Implement discovery that:

1. locates the CMS monthly page for the requested month
2. scans the page for a ZIP link
3. validates that the ZIP likely corresponds to the requested month
4. downloads it

If discovery fails, provide a clear error.

---

## Parsing Requirements

The parser must be defensive.

Requirements:

* detect file types by headers and structure, not just exact names
* support future minor CMS layout changes where possible
* normalize header names into internal canonical names
* validate presence of required fields before processing

Canonical internal fields should include at minimum:

* `contract_id`
* `plan_id`
* `plan_name`
* `state_code`
* `county_name`
* `enrollment`
* any other plan metadata needed from source

If the ZIP includes multiple relevant files, create internal typed row models and join them only as needed during streaming ingestion.

---

## Joining Logic

If source data requires joining contract metadata and enrollment detail:

* join using `contract_id + plan_id`
* do not materialize a giant permanent joined dataset on disk
* do not persist a raw exploded output as the primary storage format
* use join results only to resolve dimensions and series updates

This is a critical design rule.

---

## Merge Logic

When ingesting a month:

1. load manifests and dimensions
2. parse source rows
3. skip `*` rows
4. resolve or create `plan_key`
5. resolve or create `county_key`
6. accumulate updates in a temporary in-memory structure keyed by `(plan_key, county_key)`
7. merge those updates into persisted series records
8. write only touched partitions
9. update manifest and ingestion log

Temporary update structure example:

```rust
HashMap<(u32, u32), u32>
```

where the value is the enrollment for the month currently being ingested.

For a multi-month range, process one month at a time.

---

## Validation Requirements

Each ingest run must create a validation summary including at least:

* requested month
* discovered source URL
* source hash
* total parsed rows
* total kept rows
* total skipped `*` rows
* total malformed rows
* number of plans resolved
* number of counties resolved
* number of series touched
* success/failure status

Also validate:

* ZIP integrity
* required headers present
* no duplicate month slot exists for the same `(plan_key, county_key)` after merge

---

## Manifest Requirements

Maintain machine-readable manifest files.

### `months.json`

Example shape:

```json
{
  "ingested_months": ["2025-01", "2025-02", "2026-03"],
  "source_hashes": {
    "2026-03": "sha256:..."
  }
}
```

### `ingestion_log.json`

Track validation and ingestion summaries over time.

### Optional key lookup manifest

Maintain efficient lookup structures or persisted key maps if that simplifies startup and incremental updates.

---

## Query API Requirements

Implement internal Rust query methods that can later be wrapped by a local web app or API.

Must support:

### Plan trend query

Input:

* contract ID
* plan ID
* optional state
* optional county
* month range

Output:

* ordered month series
* enrollment values

### County snapshot query

Input:

* state
* county
* month

Output:

* plans in county for that month
* enrollment values

### State rollup query

Input:

* state
* month range

Output:

* total enrollment by month
* optional plan-level breakdown

### Top movers query

Input:

* state and/or county
* month A
* month B

Output:

* largest positive and negative enrollment changes

---

## Performance Expectations

Design for the following priorities:

1. compact disk usage
2. fast month append / incremental ingestion
3. very fast read access for future visualizations
4. minimal repeated strings in storage
5. ability to query subsets without scanning everything

Use integer surrogate keys wherever possible in the analytical layer.

---

## What Not To Do

Do not do the following:

1. Do not persist the raw CMS files as the primary query layer.
2. Do not store plan name, county name, and state text redundantly for every month row.
3. Do not build a massive permanent fully joined row-per-month table as the main storage structure.
4. Do not assume plan names never change.
5. Do not silently discard malformed data without logging it.
6. Do not tightly couple the storage layer to a web framework.

---

## Desired Deliverables

Build the project in phases.

### Phase 1: working ingestion MVP

Deliver:

* CLI scaffolding
* month discovery and download
* ZIP parsing
* row normalization
* skip `*` rows
* dimension creation
* compact series persistence in Parquet
* manifests and validation log
* basic query commands

### Phase 2: robustness and incremental optimization

Deliver:

* dimension versioning for plan metadata changes
* better partition pruning
* replace / force reingest logic
* stronger tests
* binary serving cache

### Phase 3: future analysis readiness

Deliver:

* query APIs optimized for a local web app
* optional DuckDB interoperability helpers
* rollups and top mover calculations
* export helpers for downstream UI

---

## Testing Requirements

Write tests for at least:

1. month parsing and validation
2. CMS page discovery logic
3. ZIP extraction
4. header normalization
5. starred enrollment row skipping
6. plan key resolution
7. county key resolution
8. plan versioning behavior when metadata changes
9. merge logic for appending a new month
10. query correctness for trend and snapshot retrieval

Also add at least one end-to-end test using a small fixture ZIP.

---

## Pseudocode Reference

Use this as conceptual guidance, not strict syntax.

```rust
fn ingest_month(month: YearMonth) -> Result<()> {
    let source_info = cms::discover::discover_month(month)?;
    let zip_bytes = cms::download::download(&source_info.zip_url)?;
    let sha = util::hashing::sha256(&zip_bytes);

    let extracted = cms::parse::extract_zip(&zip_bytes)?;
    let source_layout = cms::parse::detect_layout(&extracted)?;

    let mut updates: HashMap<(u32, u32), u32> = HashMap::new();
    let mut stats = IngestStats::default();

    for row in source_layout.stream_rows()? {
        stats.total_rows += 1;

        let normalized = ingest::normalize::normalize_row(row)?;

        if normalized.enrollment_is_star() {
            stats.star_rows += 1;
            continue;
        }

        let enrollment = match normalized.parse_enrollment() {
            Ok(v) => v,
            Err(_) => {
                stats.malformed_rows += 1;
                continue;
            }
        };

        let plan_key = ingest::resolver::resolve_plan_key(&normalized)?;
        let county_key = ingest::resolver::resolve_county_key(&normalized)?;

        updates.insert((plan_key, county_key), enrollment);
        stats.kept_rows += 1;
    }

    storage::parquet_store::merge_month(month, &updates)?;
    storage::manifests::record_success(month, sha, stats)?;
    storage::binary_cache::refresh_touched_segments()?;

    Ok(())
}
```

---

## Coding Standards

Follow these rules while implementing:

* keep modules small and focused
* prefer explicit types over overly clever abstractions
* avoid unnecessary macros
* document key data model decisions in comments
* make file and module names obvious
* keep I/O boundaries clear
* return actionable errors
* write enough tests that reingestion and merge logic are trustworthy

---

## Final Instruction

Build the first working version now.

Prioritize correctness, compact storage, and clean architecture over exotic compression tricks.

The first version must successfully:

* ingest selected CMS months
* skip `*` rows
* deduplicate repeated plan and county metadata
* store monthly enrollments in compact per-plan-per-county series
* support incremental month additions
* expose fast query-ready structures for a future analysis app

When making tradeoffs, choose the design that best supports a future local web app needing fast trends, county comparisons, and state rollups over many months without row explosion.

