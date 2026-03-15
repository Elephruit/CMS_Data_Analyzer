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

## UI Implementation Spec for CLI Coder

Use this section as the implementation reference for the frontend and UI-facing query layer that will sit on top of the completed backend store.

The UI should be built in **chunks**. Each chunk must be independently implementable and testable. Do not jump ahead and build scattered components without first establishing the shared application shell, data contracts, and filter state model.

The app should feel like a premium analytics product: fast, modern, visually polished, and built for deep exploration of CMS enrollment data.

---

## UI Product Goal

Build a local analytical web application that sits on top of the Rust data store and lets a user:

1. add new months to the dataset
2. remove stored months
3. explore enrollment by parent organization, contract, plan, state, county, and plan type
4. analyze month-over-month growth
5. analyze AEP growth
6. use content-aware filters that dynamically shrink available choices based on the current selection context
7. export current tables and chart source data to CSV

The UI must be state-of-the-art in usability and visual quality while remaining highly performant on large result sets.

---

## UI Design Principles

1. Build dark-mode first.
2. Favor a premium analytics aesthetic over a generic admin dashboard.
3. Use smooth state transitions and instant filter feedback.
4. Keep global filters persistent and obvious.
5. Every visible metric must respond to current filters.
6. Every major table must be exportable to CSV.
7. Every chart should be able to export its source data to CSV.
8. Drill-down should be intuitive and require minimal clicks.
9. Definitions such as AEP growth must be explicit in the interface.
10. Support dense analytical workflows without visual clutter.

---

## Required UI Sections

Implement the UI around these major sections:

1. Dashboard
2. Data Management
3. Enrollment Explorer
4. Parent Organization Analysis
5. Plan Analysis
6. Geography Analysis
7. Growth & AEP Analysis
8. Exports

All sections must share the same global filter model.

---

## Shared Application Shell Requirements

Before building any analytical page, implement the shared app shell.

### App shell must include

* left navigation rail
* top header bar
* global filter bar under the header
* main content region
* compare tray or compare state support for future use
* export actions available at page and component level where relevant

### Left navigation items

* Dashboard
* Enrollment Explorer
* Parent Organizations
* Plans
* Geography
* Growth & AEP
* Data Management
* Exports

### Top header must support

* current dataset range summary
* quick search
* global export entry point
* user feedback area for ingest status, validation status, and success/error notifications

---

## Global Filter Model Spec

This is a core system and must be built early.

### Global filters to support

* month range
* state
* county
* parent organization
* contract
* plan
* plan type
* product type if available
* SNP or non-SNP if available
* free text search

### Content-aware behavior

All filters must be content aware.

Examples:

* if state = Michigan, only show parent organizations, counties, contracts, and plans that exist in Michigan
* if state = Michigan and plan type = PPO, only show parent organizations and plans that offer PPO in Michigan
* if county = Jackson, only show plans and organizations present in Jackson

### Filter UI requirements

Each filter must support as appropriate:

* multi-select
* searchable values
* clear action
* current selection chips
* count display next to values when feasible

Example filter value display:

* Humana `(128,442)`
* UnitedHealthcare `(117,005)`
* BCBSM `(96,221)`

Counts should update based on current filter context.

### Filter state requirements

* filters must be centrally managed
* all pages must subscribe to the same filter state model
* page-local secondary controls are allowed, but the global filters are authoritative
* URL state or equivalent deep-linkable state is strongly preferred

---

## Data Contracts Required From Backend

The frontend implementation assumes the backend can provide fast query responses. The CLI coder should also define the API or local query contract needed for these views.

At minimum, support data contracts for:

1. dataset summary
2. loaded month list
3. ingest month action
4. remove month action
5. global filter option resolution
6. dashboard KPI summary
7. trend series query
8. ranked table query
9. parent organization comparison query
10. plan detail query
11. geography summary query
12. month-over-month growth query
13. AEP growth query
14. CSV export query

Do not build the UI assuming raw full-table scans in the browser.

---

## Chunked UI Implementation Plan

Implement in the following chunks, in order.

---

## Chunk 1: Application Shell and Frontend Foundation

### Goal

Create the visual and architectural base of the app.

### Deliverables

* React app scaffold
* routing structure
* left navigation
* top header
* main page container layout
* dark theme foundation
* shared spacing, typography, color tokens, and card styles
* reusable panel, KPI card, table wrapper, filter chip, and empty-state components

### Functional requirements

* app starts with a responsive shell
* all nav items route correctly
* placeholder pages exist for all major sections
* theme is cohesive and modern
* shell supports persistent global filters area even if filters are not yet fully wired

### Acceptance criteria

* user can navigate between all main pages
* shell visually looks production-grade, not bare scaffolding
* reusable primitives are in place before feature pages are built

---

## Chunk 2: Global Filter Engine and Query Context

### Goal

Build the content-aware filter framework that powers the entire application.

### Deliverables

* central filter store
* filter bar UI
* multi-select searchable filter components
* filter chip summary area
* dynamic option loading logic
* page subscription to filter state

### Functional requirements

* selecting a filter updates all dependent filters
* filter values shrink intelligently based on current context
* counts next to values update with context
* clear-all and per-filter clear actions work
* current selection is visible as chips or breadcrumbs

### Acceptance criteria

* selecting state changes available counties, parent organizations, contracts, plans, and plan types
* selecting plan type further narrows parent organizations and plans
* filters can be applied, removed, and combined without UI inconsistency
* pages can consume the current filter state

---

## Chunk 3: Data Management Screen

### Goal

Provide a polished UI for adding and removing months from the analytical store.

### Deliverables

* Data Management page
* loaded months table
* add month panel
* add month range flow
* remove month action
* ingest progress/status display
* data health summary cards
* validation log summary display

### Functional requirements

#### Loaded months area

Display at minimum:

* month
* ingest date
* source hash or source identifier
* total parsed rows
* kept rows
* skipped starred rows
* status
* actions

Actions:

* remove month
* refresh/reingest month
* inspect validation details

#### Add months flow

User can:

* select one month
* select a month range
* preview what will be ingested
* trigger download and add
* see progress and success/failure result

#### Remove month flow

User can:

* remove a stored month
* see a confirmation warning
* understand that removing a month affects trends and growth metrics

### Acceptance criteria

* user can add a month and see the loaded month list refresh
* user can remove a month and see dependent metadata refresh
* status messages are clear and trustworthy
* the page feels like a polished control center, not a raw admin table

---

## Chunk 4: Dashboard

### Goal

Create the high-level landing page for monitoring the loaded dataset and current filtered market view.

### Deliverables

* KPI row
* enrollment trend chart
* top parent organizations chart
* geographic summary visual placeholder or initial map
* AEP leaderboard table/card section

### KPI cards

At minimum:

* total enrollment
* total parent organizations
* total contracts
* total plans
* states loaded
* counties loaded
* months stored

### Functional requirements

* all KPI values respond to current global filters
* clicking a KPI can optionally refine or navigate to deeper analysis
* trend chart shows enrollment over time for the current filtered scope
* top parent organizations chart ranks current filtered enrollment and optionally shows growth
* AEP leaderboard shows growth using explicit AEP definition

### Acceptance criteria

* dashboard loads quickly
* all components respect global filters
* page gives immediate situational awareness
* layout looks executive-ready and analytically useful

---

## Chunk 5: Enrollment Explorer

### Goal

Build the main all-purpose analysis page for flexible slicing across dimensions.

### Deliverables

* summary metric cards
* main trend chart
* composition chart
* grain toggle
* interactive ranked table
* CSV export for table and chart source data

### Required grain toggle options

* parent organization
* contract
* plan
* county

### Required table metrics

* current enrollment
* prior month enrollment
* month-over-month change
* month-over-month percent change
* Dec prior year enrollment when applicable
* Feb following year enrollment when applicable
* AEP growth
* AEP growth percent
* counts such as plan count, county count, or state count when relevant to grain

### Functional requirements

* user can switch grain and table re-renders correctly
* trend chart follows current filters and selected comparison state
* table supports sort, search, pagination or virtualization, and CSV export
* chart source data can be exported

### Acceptance criteria

* page acts as a strong exploratory workspace
* changing grain does not break metrics
* table and chart stay aligned to same filter context

---

## Chunk 6: Parent Organization Analysis

### Goal

Provide a dedicated comparison and drill-down experience for parent organizations.

### Deliverables

* parent organization selector
* comparison cards
* trend comparison chart
* plan type distribution chart
* geography footprint panel
* underlying contracts/plans table

### Functional requirements

* user can compare multiple parent organizations
* summary cards show current enrollment, number of plans, states active, counties active, MoM growth, and AEP growth
* distribution chart shows plan type mix
* contracts/plans table shows drill-down details and supports export

### Acceptance criteria

* page makes competitive comparison easy
* multi-org comparison is visually clear
* all metrics stay aligned with global filters plus selected organizations

---

## Chunk 7: Plan Analysis

### Goal

Provide detailed plan-level analysis and county footprint exploration.

### Deliverables

* plan search and plan selection UI
* plan summary card
* total enrollment trend chart
* county distribution table
* month-over-month change visual
* CSV export

### Functional requirements

* user can search plan by plan name or contract-plan combination
* user can see plan metadata and current enrollment scope
* county table shows where the plan exists and how it performs across geographies
* trend chart reflects selected plan and global filters

### Acceptance criteria

* page is useful for product-specific analysis
* county detail is easy to interpret
* export behaves correctly for filtered plan view

---

## Chunk 8: Geography Analysis

### Goal

Build state-first and county-first geographic exploration.

### Deliverables

* geography mode toggle for state vs county
* state summary panels
* county summary panels
* map component or visual geography panel
* top geographies table

### Functional requirements

#### State mode

Show:

* total enrollment in state
* parent organization share
* plan type mix
* top counties
* growth over time
* AEP growth by parent organization inside the state

#### County mode

Show:

* plans in county
* parent organization ranking
* county trend over time
* plan type distribution
* top gainers and decliners

### Acceptance criteria

* selecting geography filters the rest of the page appropriately
* state and county views are meaningfully different, not just relabeled copies
* geography exploration supports drill-down

---

## Chunk 9: Growth & AEP Analysis

### Goal

Create a dedicated section for growth analysis rather than treating growth as only a secondary metric.

### Deliverables

* month-over-month KPI cards
* month-over-month ranked gainers and decliners table
* AEP year selector
* AEP KPI cards
* AEP comparison table
* optional scatter or bubble chart for advanced AEP analysis

### AEP logic

AEP growth must be defined in the UI exactly as:

**AEP growth = February of the following year minus December of the prior year**

Example helper text:

**2025 AEP growth = Feb 2025 minus Dec 2024**

### Functional requirements

* user can select an AEP year
* calculations update correctly for current global filters
* table supports grain switching where appropriate
* top movers and largest declines are visible

### Acceptance criteria

* AEP logic is clearly explained and accurately implemented
* month-over-month and AEP analytics feel like first-class features

---

## Chunk 10: Export System

### Goal

Make exports a first-class workflow across the app.

### Deliverables

* component-level CSV export actions
* page-level export action
* export payload description text
* reusable export utility

### Functional requirements

Exports must reflect:

* current filters
* current sort
* current grain
* current date range
* current page context

### Example export descriptor

* `Parent Organization Analysis | Michigan | PPO | Jan 2025 to Feb 2026`

### Acceptance criteria

* every major table can export to CSV
* chart source data can export to CSV
* exported content matches what the user is viewing

---

## Chunk 11: Compare Mode and Cross-Page Drilldowns

### Goal

Improve analytical flow across pages.

### Deliverables

* compare tray or compare state UI
* click-through drill-down patterns
* page-to-page navigation with preserved filter context

### Functional requirements

* user can select entities to compare and carry them across multiple views
* clicking a chart or table row can navigate into a deeper page with context preserved
* filter state persists across related drill-down workflows

### Acceptance criteria

* analysis feels fluid rather than page-isolated
* drill-down requires very few clicks

---

## Chunk 12: Polish, Performance, and UX Refinement

### Goal

Bring the product to a premium quality level.

### Deliverables

* loading skeletons
* polished empty states
* inline metric definitions
* better transitions and microinteractions
* table virtualization where needed
* performance optimization for large result sets
* error and warning banners that are clear but elegant

### Functional requirements

* app remains responsive with large datasets
* users always know what the current context is
* metric definitions are discoverable
* empty states never feel broken or unfinished

### Acceptance criteria

* app feels production-ready and premium
* performance remains strong under realistic dataset size
* UX friction is low across all major workflows

---

## Shared UI Component Requirements

Build reusable components early where possible.

### Required shared components

* app shell
* nav rail
* top header
* global filter bar
* filter chip list
* KPI card
* chart card wrapper
* export button
* searchable multi-select filter
* ranked data table wrapper
* empty state
* loading skeleton
* confirmation modal
* toast/notification system

Do not build one-off versions of these on every page.

---

## Visual Design Direction

The interface should feel like a premium analytics platform.

### Design expectations

* dark-first UI
* layered panels/cards
* rounded corners
* clean typography hierarchy
* subtle motion
* restrained but rich color use
* dense but readable analytical layout
* chart styling that feels modern and presentation-ready

Avoid a plain enterprise-gray look. The UI should be visually impressive without sacrificing clarity.

---

## CSV Export Requirements

CSV export is required throughout the product.

### Exportable items

* loaded months table
* dashboard leaderboard tables
* ranked analysis tables
* parent organization tables
* plan detail tables
* geography tables
* growth tables
* chart source data for major visuals

### Export rules

* export current view only unless explicitly offering full-export options
* include enough column detail for downstream spreadsheet use
* preserve current filtering context in export metadata or filename when possible

---

## Metrics and Definitions Requirements

Definitions must be visible in the UI where appropriate.

At minimum define clearly:

* current enrollment
* prior month enrollment
* month-over-month growth
* month-over-month percent growth
* AEP growth
* AEP growth percent

AEP must always be described consistently.

---

## Routing and State Requirements

* pages must be routable
* filter state should be shareable via URL or equivalent serializable state
* drill-downs should preserve current context
* compare selections should persist during a session

---

## Testing Requirements for UI

Write tests for at least:

1. navigation shell rendering
2. filter dependency behavior
3. add/remove month workflows
4. dashboard metric rendering under filters
5. AEP calculation display logic
6. table export behavior
7. page drill-down with preserved filters
8. content-aware option narrowing

Add at least one end-to-end test that simulates:

* selecting a state
* selecting a plan type
* seeing parent organization options narrow
* opening an analysis page
* exporting the visible table

---

## Final Instruction

Implement the UI in chunks exactly in the order above.

Do not skip the shared shell or global filter system. Those are foundational.

Prioritize:

1. coherent shared filter behavior
2. premium analytical UX
3. fast drill-down and comparisons
4. correct MoM and AEP metrics
5. CSV export from every important analytical surface

The finished product should feel like a modern, beautiful, high-performance enrollment analytics platform rather than a simple data browser.
