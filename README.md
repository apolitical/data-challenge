# Apolitical Data Challenge

This repository contains technical challenges for data roles at Apolitical.

## Challenges

| Challenge | Target Role | Branch | Time |
|-----------|-------------|--------|------|
| [Data Lead](../../tree/data-lead) | Data Lead / Senior Data Engineer | `data-lead` | ~1.5–2 hours |
| [Analytics Engineer](../../tree/analytics-engineer) | Analytics Engineer | `analytics-engineer` | ~1.5 hours |

## Quick Start

You should have received instructions on which challenge to complete.
Clone the appropriate branch:

```bash
git clone -b <branch-name> <repo-url>
```

Then follow the README in that branch.

## Shared Infrastructure

Both challenges use:
- **DuckDB** as the local database engine
- **dbt** for data modeling
- **uv** as the Python package manager
- Raw data in `mock_data/` loaded via `scripts/init_db.py`

## Branch Policy

Challenge branches (`data-lead`, `analytics-engineer`) must **not** be merged into `main`.
Each branch is self-contained with its own README, setup instructions, and task definitions.
