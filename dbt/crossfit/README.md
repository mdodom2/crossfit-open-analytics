# CrossFit Open Analytics dbt Project

## Overview
This project models CrossFit Open leaderboard data in Snowflake using dbt and a medallion-style architecture. The pipeline ingests raw leaderboard payloads, standardizes athlete and workout-level records, and publishes a dimensional Gold layer for analytics.

## Objective
The primary objective of this project is to transform raw CrossFit Open leaderboard API payloads into a clean, documented analytical model that supports athlete, workout, competition, division, scaled-type, and region analysis.

## Architecture

### Bronze
Raw source capture from the CrossFit Open leaderboard payload.

**Model(s):**
- `bronze_crossfit__open_leaderboard_raw`

**Purpose:**
- Preserve raw leaderboard payloads
- Retain source lineage and audit columns
- Support replayability and downstream flattening

### Silver
Refined and flattened views built from the Bronze payload.

**Model(s):**
- `silver_open_leaderboard`
- `silver_open_workout_scores`

**Purpose:**
- Flatten nested JSON arrays
- Standardize business keys
- Type-cast source attributes
- Prepare clean analytical records for Gold dimensions and facts

### Gold
Dimensional star schema for reporting and analytics.

**Dimensions:**
- `gold_dim_athlete`
- `gold_dim_competition`
- `gold_dim_workout`
- `gold_dim_division`
- `gold_dim_scaled_type`
- `gold_dim_region`

**Fact:**
- `gold_fct_open_workout_scores`

**Purpose:**
- Provide reusable conformed dimensions
- Expose one central fact table at athlete + competition + workout grain
- Support leaderboard, performance, regional, and participation analysis

## Grain Summary

| Model | Grain |
|---|---|
| `silver_open_leaderboard` | one row per athlete per competition slice |
| `silver_open_workout_scores` | one row per athlete + workout |
| `gold_dim_athlete` | one row per athlete |
| `gold_dim_competition` | one row per competition slice |
| `gold_dim_workout` | one row per competition slice + workout |
| `gold_fct_open_workout_scores` | one row per athlete + competition slice + workout |

## Key Business Mappings

### Division
- `1` = Men
- `2` = Women
- `18` = Men 35-39

### Scaled Type
- `0` = Rx
- `1` = Scaled

### Region
- `0` = Worldwide

## Data Quality
The project includes dbt tests for:
- surrogate key uniqueness
- business key uniqueness
- required not-null dimensional and fact keys

## Core Analytical Use Cases
This model supports questions such as:
- How many athletes participated by division, year, and scaled type?
- How do workout ranks vary by region or affiliate?
- Which athletes have the strongest overall placement across workouts?
- How does participation differ between Rx and Scaled divisions?
- What regional patterns emerge in CrossFit Open performance?

## Repository Structure

```text
models/
  bronze/
    crossfit/
  silver/
  gold/
    dimensions/
    facts/