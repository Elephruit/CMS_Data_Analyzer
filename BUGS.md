# Bugs and UX Fixes

This file captures current bugs, broken behaviors, and unacceptable UX/performance issues that must be fixed. These are not minor polish items. They directly impact usability, trust, and analytical value.

## 1. Market enrollment trend is not showing all loaded prior months

### Problem
On the dashboard view, the market enrollment trend should display **all loaded months up to and including the selected analysis month**.

Example:
- Loaded months: December 2024, January 2025, February 2025
- Selected analysis month: February 2025

Current behavior:
- The chart only shows January 2025 and February 2025

Expected behavior:
- The chart should show:
  - December 2024
  - January 2025
  - February 2025

### Required fix
- Ensure the dashboard market enrollment trend includes **every loaded month prior to the selected analysis month**
- Do not arbitrarily drop the earliest loaded month if it falls within the valid comparison period
- Trend logic should be based on loaded historical months, not just a limited rolling subset unless explicitly designed otherwise

---

## 2. Remove unused or nonfunctional UI elements

### Problem
The following items are not useful and should be removed:
- `Geographies`
- `View all insights`

`View all insights` currently does nothing when clicked.

### Required fix
- Remove `Geographies` from the UI
- Remove `View all insights` from the UI
- Do not leave dead controls or placeholder interactions in production-facing views

---

## 3. Enrollment value display formatting is poor for sub-million values

### Problem
Enrollment display formatting does not make sense when values are below 1 million.

Example:
- `0.03M` is much harder to interpret than `30K`

### Expected behavior
- Large values should be abbreviated intelligently
- Formatting should adapt based on scale

### Required fix
Use human-friendly formatting rules such as:
- `1,000,000+` → `1.2M`
- `1,000 to 999,999` → `30K`, `245K`, etc.
- Small values should remain readable and intuitive

Avoid showing awkward values like:
- `0.03M`
- `0.4M`

These should instead display as:
- `30K`
- `400K`

---

## 4. Market enrollment graph hover tooltip needs formatting fixes

### Problem
The hover popup on the market enrollment graph is raw and unpolished.

Current behavior:
- Date shows as `2025-01`
- Label shows lowercase `enrollment`
- Values are not comma formatted

### Expected behavior
Example tooltip:
- `January 2025`
- `Enrollment: 30,573`

### Required fix
- Convert date labels from raw `YYYY-MM` to full month name + year
- Capitalize `Enrollment`
- Add comma formatting to numeric values
- Tooltip should feel polished and presentation-ready

---

## 5. Filter interactions are too slow and unusable for rapid selection

### Problem
When using filters, selecting one option causes the rest of the options to disappear and show a temporary syncing/loading state before returning.

Current behavior:
- Click one filter option
- Other options vanish
- UI shows something like “syncing options”
- Takes noticeable time to refresh
- Prevents rapid multi-select behavior

This is unacceptable. The UI should feel instant.

### Expected behavior
- Users should be able to click multiple filter options in rapid succession with virtually no delay
- Filter controls should remain visible and stable while selections are being made
- No blocking spinner or disruptive refresh should happen during normal selection flow

### Required fix
- Eliminate blocking filter re-render behavior
- Do not temporarily remove available options after each click
- Precompute or cache valid filter option relationships so they update immediately
- Make multi-select interactions feel instantaneous
- Support fast “click click click” selection without waiting on UI synchronization after each choice

### Performance requirement
Filter interactions must feel **lightning fast**, with effectively zero perceived delay for normal use.

---

## 6. Nonfunctional upper-right icons should be removed or implemented

### Problem
In the upper-right area, there are controls/icons that appear to do nothing:
- `JD`
- bell icon
- info icon

These create confusion because they look interactive but do not appear to have meaningful behavior.

### Required fix
Choose one of the following:
- Remove them entirely if they are not needed
- Fully implement them if they are intended to be functional

Do not leave decorative or fake-interactive controls in the interface.

---

## 7. Data Management tab has poor typography, spacing, and alignment

### Problem
The Data Management tab feels visually unrefined and harder to read than it should be.

Issues:
- Fonts on the cards are too small
- Text is hard to read
- There is too much empty space
- Month display is redundant and awkward
- Loaded-state icons are misaligned

Example:
- There is no need to show both `February` and `02`
- When a month is loaded, the blue check and trash icon are not aligned and look sloppy/unprofessional

### Required fix
- Increase font sizes for readability
- Improve spacing and visual density so cards feel intentional rather than sparse
- Simplify month display to one clear representation
- Remove redundant month number text if month name is already shown
- Align loaded-state controls/icons properly
- Ensure the loaded indicator, check icon, and trash/delete icon sit cleanly on the same visual line and look polished

The Data Management tab should look deliberate, modern, and professional.

---

## 8. Remove backend/infrastructure language from end-user UI

### Problem
The UI currently includes backend-oriented technical language such as:

> Provision and manage multi-year CMS datasets. High-performance Parquet storage management.

This is inappropriate for the end user.

### Why this is a problem
The end user:
- does not care about backend architecture
- may not understand terms like Parquet storage
- should not be exposed to infrastructure implementation details in product-facing UI copy

### Required fix
- Remove backend/infrastructure wording from user-facing screens
- Replace with plain language focused on user goals and actions
- Keep technical implementation details out of the UI unless they are explicitly relevant to the user

Copy should describe what the user can do, not how the storage layer works.

---

## 9. Growth and AEP tab is unclear and far too slow

### Problem
The Growth and AEP tab is confusing and extremely slow.

Issues:
- It is not obvious what the tab is trying to show
- The purpose of the visuals and data is unclear
- Changing a filter caused approximately 15 seconds of loading/spinning before updating

That level of delay is totally unacceptable.

### Required fix
#### Clarity
- Redesign or relabel the tab so the purpose is obvious
- Make it immediately understandable what metrics are being shown
- Improve headings, labels, chart descriptions, and explanatory context
- If the tab is showing AEP growth logic, define it clearly in the UI

#### Performance
- Filter changes on this tab must update dramatically faster
- Investigate why this view is so much slower than acceptable
- Remove unnecessary recomputation, re-querying, or expensive rendering work
- Cache, memoize, pre-aggregate, or pre-index anything needed to make this responsive

### Performance requirement
A filter change should not result in a 15-second spinner. That is unacceptable for this application.

---

# Overall expectation

This application is supposed to feel **extremely fast, polished, and analytically powerful**.

Current issues suggest:
- too many placeholder or dead UI elements
- weak formatting and presentation
- slow filter interactions
- expensive updates and re-renders
- unclear analytical views

## Global directive
Treat the following as top-level requirements across the app:
- fast, responsive interactions
- polished formatting
- no dead controls
- no backend jargon in user-facing UI
- charts and tabs must be immediately understandable
- filtering must feel instantaneous