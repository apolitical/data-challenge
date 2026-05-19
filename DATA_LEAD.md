# Data Lead Challenge

Timebox: **90 minutes**

This is a live coding exercise. You'll share your screen and talk through your decisions as you go. AI tools (Claude Code, Cursor, Copilot, ChatGPT, etc.) are welcome, we're interested in how you use them as much as in what you produce.

## What to expect

| Time | What happens |
|---|---|
| 0-15 min | You read the email below and the legacy SQL in `1-sql/messy_course_engagement_duckdb.sql`. We then ask you to talk through what the SQL is doing and what concerns you have. No coding yet. |
| 15-60 min | You build out the dbt project. AI tools welcome. |
| 60-80 min | Follow-up questions about your work, plus a quick self-review: "imagine this was a colleague's PR, what would you call out?" |
| 80-90 min | Your turn: any questions you have for us. |

We care more about clear thinking, sensible trade-offs, and your judgment than about polished output. If anything in the brief below is unclear, just ask, the interviewer is happy to clarify on Sam's behalf.

## The scenario

You've just joined as data lead. A legacy SQL query (`1-sql/messy_course_engagement_duckdb.sql`) produces a course engagement view that Sam from Learning Ops relies on. Sam wants you to take it over and turn it into something cleaner. You've received the following email from them:

---

> **From:** Sam Patel <sam.patel@example.com>
> **To:** You
> **Subject:** taking over the course engagement query, quick brief
>
> Hi,
>
> Welcome aboard! Really glad someone's owning this properly now.
>
> Quick context on what I need from the course engagement data. We use it for the weekly board pack, so the headline thing is **one row per course** showing how each course is doing.
>
> Things I usually look at:
>
> - how many learners we've got on each course
> - active learners, the ones really using the platform. I tend to think of this as people who've done something in the last 30 days, but I'm flexible if you've got a better definition
> - whether they're getting through the content (videos, quizzes, that sort of thing)
> - when activity is happening
>
> Honestly, "course health" is the vibe. If you spot anything in the existing query that doesn't match what I just described, flag it, I haven't looked at the SQL in months and it might be doing something different.
>
> The existing query works (I think?), it's just hard to maintain. Could you turn it into something cleaner using dbt? A few light tests would be nice too.
>
> Shout if anything's unclear.
>
> Thanks!
> Sam

---

## Setup

1. Install `uv`.
2. Run `uv sync`.
3. Run `uv run python scripts/init_db.py`.
4. Optional: `uv run jupyter lab` to explore the data interactively.

See `SETUP.md` for the full setup guide, dbt commands, and Airflow commands.

## Repository map

- `mock_data/`: sample CSV inputs
- `1-sql/messy_course_engagement_duckdb.sql`: legacy query, kept as a read-only reference
- `2-dbt_project/`: dbt project skeleton, including an editable copy of the legacy query at `models/base/legacy.sql`
- `3-airflow/dags/pipeline.py`: Airflow DAG skeleton (stretch goal only)
- `notebooks/example_data_exploration.ipynb`: optional exploration notebook

## The task

Turn the legacy SQL into a small, clean dbt pipeline that answers Sam's brief.

- Use the `2-dbt_project/` skeleton: `models/base/`, `models/intermediate/`, `models/marts/`.
- Use `source()` and `ref()` so the model dependencies are clear.
- The final mart should be **one row per course** (per Sam's anchor).
- Decide what metrics to compute and how to define them. Be ready to justify your choices.
- Add a small amount of testing (`unique`, `not_null`, and a `relationships` test) and short descriptions on the final mart.

You are **absolutely free to edit** `2-dbt_project/models/base/legacy.sql`, which is an editable copy of the messy query already wired up to `{{ source(...) }}` refs. Treat it as working scratch code, not something you need to preserve. You can simplify it, rewrite parts of it, or break it into smaller pieces as you work out the logic. The original `1-sql/messy_course_engagement_duckdb.sql` is a read-only reference, please leave it alone.

## Stretch goal: Airflow

If you finish with time to spare, open `3-airflow/dags/pipeline.py` and wire the dbt models behind an Airflow DAG. There are TODOs in the skeleton: implement `run_base`, `run_intermediate`, `run_marts`, and `report_data` (export `analytics.marts_courses__engagement` to `output/reports/course_engagement_{date}.csv`), and chain them.

This is **optional**. We'd rather see a thoughtful dbt model with one or two well-chosen tests than a rushed mart plus a half-working DAG.

## Helpful references

- `SETUP.md`
- `notebooks/example_data_exploration.ipynb`
- [dbt docs](https://docs.getdbt.com/)
- [Airflow TaskFlow API docs](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
