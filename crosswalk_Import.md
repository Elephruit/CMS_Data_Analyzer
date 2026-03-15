# CMS Data Ingestion Expansion – Plan Crosswalk + Plan Lineage + AEP Switching

## Objective

Extend the application to ingest **CMS Plan Crosswalk datasets** and integrate them into the analytics platform.

The Plan Crosswalk dataset describes how Medicare Advantage and Part D plans transition between contract years. It identifies:

- plan renewals
- consolidations
- service area expansions
- service area reductions
- new plans
- new contracts
- terminated plans

This dataset is essential for correctly interpreting **year-over-year plan changes**.

Currently this data is **not present in the Data Management tab** and must be added.

This project includes:

1. Crosswalk data ingestion
2. Crosswalk normalization across historical schemas
3. High performance storage and indexing
4. A new UI page to visualize crosswalk data
5. A **Plan Lineage Engine**
6. An **AEP Switching Estimator**

All components must integrate with the existing application architecture.

---

# Data Source

CMS publishes plan crosswalk files here:

https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/plan-crosswalks

Selecting a year leads to a page like:

https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1236744

The download link typically looks like:

https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2010/plan_crosswalk_2006.zip

File types vary by year and may include:

- ZIP archives
- XLS
- XLSX
- TXT
- CSV

Inside the ZIP archive may be one or more files.

Historical datasets may use **different column names and structures**.

The ingestion pipeline must normalize these differences automatically.

---

# Functional Requirements

The system must allow users to:

Download crosswalk datasets by year  
Delete crosswalk datasets by year  
Store normalized crosswalk data locally  
Query crosswalk relationships instantly  
Integrate crosswalk data with existing analysis tools  

Performance must be optimized for **fast analytical workloads**.

---

# Crosswalk File Structure

Typical columns include:

previous_contract_id  
previous_plan_id  
previous_plan_name  
previous_snp_type  
previous_snp_institutional  

current_contract_id  
current_plan_id  
current_plan_name  
current_snp_type  
current_snp_institutional  

status  

Historical datasets may:

- change column order
- change column names
- omit some fields

The ingestion system must detect and normalize schemas.

---

# Canonical Plan Identifier

Plans should always be represented as:
plan_key = contract_id + "-" + plan_id

Examples
H0028-015
H0294-017
H0107-003

This identifier must be used to join data with:

- enrollment datasets
- landscape files
- star ratings
- plan metadata
- internal analytics

---

# Crosswalk Status Types

CMS crosswalk status values include:

Renewal Plan  
Consolidated Renewal Plan  
Renewal Plan with SAE  
Renewal Plan with SAR  
New Plan  
Initial Contract  
Terminated/Non-renewed Contract  

---

## Renewal Plan

Plan continues unchanged.

Characteristics:

- same contract id
- same plan id
- same service area

Interpretation:

Product continues unchanged.

---

## Consolidated Renewal Plan

Two or more plans combine into one.

Example
2025 H0028-068
2025 H0028-015
→
2026 H0028-015

Interpretation:

Multiple plans merged.

Enrollment analysis must aggregate predecessor plans.

---

## Renewal Plan with SAE

SAE = Service Area Expansion.

Plan retains prior counties and adds new ones.

Interpretation:

Geographic expansion.

---

## Renewal Plan with SAR

SAR = Service Area Reduction.

Plan retains only part of its prior service area.

Interpretation:

Geographic exit from one or more counties.

---

## New Plan

Plan did not exist in the prior year.

Indicator
previous_plan_id = NEW

Interpretation:

New product launch.

---

## Initial Contract

Plan offered under a brand new CMS contract.

Interpretation:

New plan sponsor entry.

---

## Terminated / Non-Renewed Contract

Plan discontinued.

Indicator
current_contract_id = TERMINATED

Interpretation:

Plan exited the market.

---

# Ingestion Pipeline

## Step 1 – Download

Add Plan Crosswalk to the **Data Management tab**.

User actions:

Download crosswalk data by year  
Delete crosswalk data by year  

Files should be cached locally.

---

## Step 2 – Extract

If dataset is zipped:

- unzip archive
- locate crosswalk file
- detect format

---

## Step 3 – Schema Detection

Because schemas vary across years, dynamically map columns.

Example mappings
PREVIOUS_CONTRACT_ID → previous_contract_id
contract_id_old → previous_contract_id

Normalize all fields to canonical names.

---

## Step 4 – Normalize Data

Convert all files into the standardized schema:
crosswalk_year
previous_contract_id
previous_plan_id
previous_plan_key
previous_plan_name
previous_snp_type
previous_snp_institutional
current_contract_id
current_plan_id
current_plan_key
current_plan_name
current_snp_type
current_snp_institutional
status

Derived fields
previous_plan_key = previous_contract_id + "-" + previous_plan_id
current_plan_key = current_contract_id + "-" + current_plan_id

---

## Step 5 – Storage

Store normalized crosswalk data in a columnar format.

Recommended structure
crosswalk/
2007.parquet
2008.parquet
2009.parquet
...
2026.parquet

Index by

- plan_key
- crosswalk_year

This ensures fast lookups.

---

# Plan Lineage Engine

## Purpose

Track a plan's evolution across multiple years even when the plan ID changes.

Example
2023 H0028-068
→ 2024 H0028-015
→ 2025 H0028-015

These should be treated as the **same product lineage**.

---

## Implementation

Construct a lineage graph.

Nodes
plan_key + year

Edges
crosswalk mapping

Compute lineage groups using graph traversal.

Output schema
plan_lineage_id
contract_id
plan_id
year
plan_key

---

## Benefits

Allows analysis such as:

Product lifecycle tracking  
True growth vs renumbering  
Strategic product evolution  

---

# AEP Switching Estimator

## Purpose

Separate **true member switching** from **structural crosswalk movement**.

Crosswalk defines the **default member migration path**.

Example
2025 H0028-068
→ 2026 H0028-015

Members moving along this path are **not considered switching**.

---

## Algorithm

1 Load enrollment data for both years  
2 Apply crosswalk mapping  
3 Determine expected enrollment migration  
4 Compare with actual enrollment  
true_switching =
actual_member_movement
crosswalk_expected_movement

---

## Outputs

Switching metrics by:

Parent organization  
Plan  
Plan type  
State  
County  

---

# UI Requirements – Crosswalk Analysis Page

Create a **new page** in the application.

This page must match the existing design language of the app.

Examples of reference pages

- Dashboard
- Enrollment Explorer
- Plans page

The page should feel **consistent with the app's theme and layout**.

---

# Page Layout

## Summary Metrics

Display cards showing

Total Renewal Plans  
Consolidations  
New Plans  
Terminated Plans  
Service Area Expansions  
Service Area Reductions  

Cards should respond to filters.

---

## Parent Organization Grouping

Group crosswalk results by **Parent Organization**.

For each parent organization display:

- renewal count
- consolidation count
- new plan count
- terminated plan count
- SAE count
- SAR count

---

## Crosswalk Relationships

Display plan transitions visually.

Examples
H0028-068 → H0028-015
H0294-017 → H0294-017

Possible formats

Flow diagrams  
Plan transition tables  
Expandable lineage views  

Goal: make plan transitions easy to interpret.

---

## Detailed Table

Provide a sortable table.

Columns

Previous Plan  
Previous Plan Name  
Current Plan  
Current Plan Name  
Status  
Parent Organization  

Features

Sorting  
Filtering  
Search  
Export to CSV  

---

# Filters

All existing application filters must apply.

Examples

State  
County  
Parent Organization  
Plan Type  
SNP Type  
Analysis Year  

Filtering should dynamically update the page.

---

# Performance Requirements

Crosswalk queries must be extremely fast.

The system should:

- pre-index crosswalk mappings
- cache results
- avoid repeated joins
- lazy load large datasets

Queries like the following should run in milliseconds:

Which plan replaced H0028-068  
Which plans consolidated into H0028-081  
Which plans terminated in 2025  

---

# Critical Analytical Rule

Never compare plan enrollment across years using raw plan IDs alone.

Always apply crosswalk mapping first.
plan → crosswalk → lineage → analysis

Without crosswalk normalization, enrollment analysis will produce incorrect conclusions about growth and switching.

---

# Final Result

Once implemented the system will be able to:

Ingest CMS crosswalk data across all years  
Normalize inconsistent schemas  
Track plan evolution using lineage  
Estimate true switching during AEP  
Visualize plan transitions grouped by parent organization  
Integrate crosswalk intelligence into the analytics platform