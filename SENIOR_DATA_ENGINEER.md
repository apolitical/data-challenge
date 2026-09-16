# Senior Data Engineer — technical exercise

**Time: about 90 minutes of work**, and **send it back by the end of the day after you
start.** You do not need an uninterrupted block — pick it up and put it down around your
week if that suits you better. We would rather see 90 minutes of work with a clear
account of what you left than a weekend of it, so tell us roughly what you spent, in
your notes. If that does not work for you, tell us and we will agree something else.

**AI tools are explicitly permitted** — use whatever you would normally use.

---

## The setup

You are on the data team that owns member engagement. A daily pipeline rebuilds each
member's **engagement tier** in the warehouse and pushes that tier into the CRM.
Marketing build their campaigns on what the CRM holds, and some of their automation
fires on a tier **changing** rather than on what it currently is.

```
 upstream feeds  ──▶   warehouse   ──▶   dbt models   ──▶    CRM    ──▶   marketing
 membership,           (DuckDB)          engagement          member        automation
 engagement                              tiers               records
```

An Airflow DAG, `sync-member-engagement`, runs that daily at 02:00 — it builds the
models, runs the tests tagged `critical`, then pushes every member's tier to the CRM.

**What an engagement tier is.** Four of them — `champion`, `engaged`, `at_risk` and
`dormant` — from most active to least. A member's tier is set from how much activity the
pipeline attributes to them; both the thresholds and the way that figure is arrived at
are in `marts_engagement__member_tiers`. The tier decides which campaign a member is in,
and the one that matters below is `dormant`.

**Where things are.**

| Path | What it holds |
|---|---|
| `2-dbt_project/` | the dbt models that produce member engagement tiers |
| `3-airflow/dags/` | the `sync-member-engagement` pipeline and its CRM client |
| `mock_crm/` | a local mock of the CRM — two environments, `development` and `production` — and its API contract |
| `mock_data/` | the committed fixtures, including one input snapshot per run date |
| `scripts/` | warehouse loading, incident replay, reset, and a standalone sync |
| `MIGRATION_MANIFEST.md` | status of the upstream engagement feed migration |

**Where this runs.** In production this is Airflow on a schedule. Here it is all local:
DuckDB for the warehouse, and two mock CRMs. There are no cloud credentials, nothing to
spend, and **nothing in this repository can reach a real service** — both CRM
environments are local mocks.

The models take a `run_date`, so you can build them as they stood on any past run date;
`SETUP.md` shows how. You can replay the incident (`scripts/run_incident.py`) and put
everything back (`scripts/reset_state.py`), as many times as you like.

---

## The incident

> **2026-09-11.** The daily `sync-member-engagement` pipeline pushed engagement tiers
> from the warehouse into the CRM, as it does every morning at 02:00.
>
> That run moved **708 of our ~12,000 members into the `dormant` tier**. Marketing's
> automation fires on the *transition into* `dormant`, so every one of them was sent the
> re-engagement email — the "we miss you, here's what you've missed" one.
>
> Nobody noticed. The pipeline ran again on the 12th, the 13th and the 14th, and
> **every run reported success.** By the 14th the automation had sent **732** of those
> emails in total.
>
> On the 14th a member replied to the email to say they had been using the platform
> that week, and please stop telling them they had gone quiet. That is how we found out
> — three days and 732 emails later.
>
> Marketing have asked us one thing: **do we need to do something about the people who
> got that email, or will it sort itself out once the pipeline is fixed?**

Today is **2026-09-15**. Nothing has been paused, and today's run has not gone yet —
it is the one you would be making.

---

## Your task

**Make this pipeline safe to run.**

That is the whole brief, and it is deliberately open. Part of what we are interested in
is how you decide what "safe" means here, what you do first, and what you consciously
leave alone.

**Show us your reasoning and decision-making around a user-facing pipeline — don't worry
about finishing everything.** One thing done and accounted for beats a list of everything
that might be wrong; **"What counts as finished" below says what "done" has to include.**

## If something is unclear

**Write down what you assumed, and carry on.** Anything about the business — how
expensive another wrong email is, how long a legitimate update can wait, who decides —
has no answer in the repository, and that is deliberate. Telling us what you assumed is
a better answer than asking us, because it shows how you would have moved without us.
Keep those assumptions in your notes and we will talk them through in the session.

If something genuinely blocks you, email us — but assume and keep going rather than
waiting for a reply.

Technical facts about how things work are already in the repository. Where they are:

| If you need to know | Look at |
|---|---|
| What the CRM does, what triggers marketing automation, rate limits | `mock_crm/API.md` |
| What each upstream feed populates | `MIGRATION_MANIFEST.md` |
| How to run everything | `SETUP.md` |
| What the source tables mean | `2-dbt_project/models/_sources.yml` |
| What the engagement mart's columns mean | `2-dbt_project/models/marts/_marts_engagement.yml` |
| DuckDB syntax you may not have used | `DUCKDB_REFERENCE.md` |

## What to send back

Your code, plus **rough notes** — what you looked at, what you concluded, what you
considered and rejected, what you knowingly left undone, and what you are unsure about.

Bullets are completely fine. Don't format it, don't write it up, don't spell-check it.
**We are assessing the reasoning, not the presentation** — so the thinking in these
notes matters, and how tidily it is written down does not. They exist so we don't spend
the interview reconstructing what you did; we would rather spend it on why.

**Notes go in a `notes/` folder** at the root. Anything you want to share and talk
through in the session goes in there — bullets, a scratch file, a query you kept, a
sketch, anything else you thought was worth raising. Any number of files, any format,
no structure required.

**Send it back as a zip** — the folder, minus `.venv/` and `mock_data.duckdb` — by
email, by the end of the day after you start.

## What counts as finished

There is more wrong here than fits in 90 minutes, and we know that. It is not enough
time to fix everything, so **we are not expecting everything fixed.**

The incident is four days old — stopping the pipeline, guarding it, or changing it are
all viable. What we do need is that you understand what happened. Whichever you choose,
show us:

- an unsafe behaviour **demonstrated**, not just suspected
- what you changed, working in the code — and what it does not cover
- what your choice costs, named — nothing here is free
- what you would fix next, in what order
- what would have to be true before you were happy for it to run tonight
- an answer to marketing's question, and what it rests on

A submission that repairs one thing well and says clearly what it left and why is a
good submission. A list of everything that might be wrong, with nothing verified and
nothing done, is not.

## The one thing we will ask you not to do

Don't spend your 90 minutes reading. There is a real warehouse here with real data in it,
and it is usually faster to ask the data a question than to work out what the code
would do.
