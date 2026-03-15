# CMS Landscape Data Import Specification

## Purpose

Add support in the Data Management system for importing **CMS Landscape files**.

Landscape files provide plan availability information and are published by CMS at:

https://www.cms.gov/medicare/coverage/prescription-drug-coverage

These files are structured differently depending on the year and must be normalized before they can be used in the application.

The system must support:

- importing the newest standalone Landscape file
- importing historical Landscape data from the multi-year archive
- normalizing inconsistent historical formats into a **single internal schema**
- managing imported years through the Data Management UI

---

# CRITICAL FIRST STEP: Historical File Structure Evaluation

Before implementing the importer, we **must analyze the structure of every historical Landscape file**.

The prior years archive (currently **2006–2025**) contains files with inconsistent structures, naming conventions, and columns.

The system **must not assume a fixed schema until this evaluation step is complete.**

### Required evaluation tasks

1. Download and extract the full historical Landscape archive.

2. For every file discovered:

   - detect file type (`csv`, `xls`, `xlsx`)
   - detect sheets if the file is Excel
   - capture column headers
   - capture row counts
   - capture a small preview sample
   - attempt to infer the year if not obvious from filename

3. Record all findings in a **machine readable manifest**.

Example:

```json
{
  "year": 2013,
  "fileName": "Landscape_Source_2013.xls",
  "sheet": "MA-PD",
  "fileType": "xls",
  "columns": [
    "State",
    "County",
    "Contract ID",
    "Plan ID",
    "Organization Name"
  ],
  "rowCountEstimate": 14500
}
Output artifacts
The discovery process must generate:
/data/landscape/manifests/landscape_manifest.json
/data/landscape/manifests/landscape_manifest_pretty.md
These artifacts will be used to design the normalization layer.
No ingestion pipeline should be implemented until this inspection step completes.
Source File Patterns
CMS publishes Landscape files in two main formats.
1. Standalone file for newest year
Example:
CY2026 Landscape (ZIP)
This usually contains:
CSV or Excel file
README or documentation
2. Historical archive
Example:
CY2006-CY2025 Landscape Files (ZIP)
This archive contains many years with inconsistent structure.
The importer must handle:
nested folders
multiple files per year
Excel or CSV formats
inconsistent column naming
Internal Normalized Schema
After evaluating the historical files, the system must normalize them into a consistent internal schema.
The baseline schema should follow the modern CMS Landscape format (2025+).
Example normalized fields:
contractYear
stateAbbreviation
stateName
countyName
contractId
planId
segmentId
parentOrganizationName
organizationMarketingName
organizationType
planName
planType
snpIndicator
snpType
partDCoverageIndicator
nationalPdp
drugBenefitType
monthlyConsolidatedPremium
partCPremium
partDBasicPremium
partDSupplementalPremium
partDTotalPremium
inNetworkMoopAmount
overallStarRating
maRegion
pdpRegion
Additional metadata fields should include:
sourceYear
sourceFile
sourceSheet
importBatchId
rowHash
If historical files contain columns not mapped to the normalized schema, they must be preserved under:
rawColumns
Parser Architecture
Because historical formats differ, implement a parser registry.
Example structure:
modern_2025_plus
legacy_2016_2024
legacy_2010_2015
legacy_2006_2009
Each parser defines:
file matching rules
sheet selection rules
column alias mapping
transformation logic
validation rules
The discovery manifest should be used to determine which parser to apply.
Data Storage
Landscape data should be stored using three layers:
/data/landscape/raw
/data/landscape/normalized
/data/landscape/manifests
Raw files and metadata must be preserved so mappings can be reprocessed later.
Data Management UI Requirements
Add Landscape as a new dataset in the Data Management interface.
The UI must show:
available years
imported years
not imported years
ability to import a year
ability to delete a year
import status
Example statuses:
Not Loaded
Loaded
Loaded With Warnings
Format Review Required
Import Failed
Import Behavior
Users must be able to:
import a single year
import multiple years
import a year from inside the historical archive
delete imported years
The importer must support:
CSV
XLS
XLSX
Nested zip structures must be supported.
Validation Rules
After ingestion:
detect duplicate rows
confirm contractId and planId formatting
verify year consistency
report unmapped columns
detect empty sheets
log warnings when inference is used
Example validation log:
{
  "year": 2014,
  "filesProcessed": 2,
  "rowsRead": 15220,
  "rowsMapped": 14987,
  "rowsRejected": 233,
  "unmappedColumns": ["Employer Group Flag"],
  "warnings": ["Plan type inferred from legacy column"]
}
Integration With Analysis Engine
Once imported and normalized, Landscape data must support filtering by:
year
state
county
parent organization
contract
plan
plan type
SNP indicators
Part D coverage indicators
The data must integrate with the existing filtering framework.
Implementation Order
Implement in the following sequence.
Step 1
Add Landscape dataset type to Data Management.
Step 2
Implement historical archive inspection.
Step 3
Generate discovery manifest.
Step 4
Design normalized schema based on discovery results.
Step 5
Implement parser registry.
Step 6
Implement year-specific import logic.
Step 7
Add validation and logging.
Step 8
Expose normalized data to analytics filters.
Key Constraints
The system must not:
assume consistent historical formats
assume one file per year
assume a fixed column set
drop unknown columns silently
Historical files must be inspected first and normalized afterward.
Success Criteria
This feature is complete when:
Landscape files can be discovered and inspected
a manifest describing historical structures is generated
at least one legacy year and one modern year import successfully
imported years can be managed from the UI
Landscape data integrates with the existing analysis engine