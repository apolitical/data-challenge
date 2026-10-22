# Senior Data Engineer: technical exercise

**Time: about 90 minutes of work, sent back by the end of the day after you start.** You
don't need an uninterrupted block. We'd rather see 90 minutes of work with a clear account
of what you left than a weekend of it, so tell us roughly how long you spent. If that
doesn't work for you, tell us and we'll agree something else.

**AI tools are explicitly permitted.** Use whatever you would normally use.

A suggested split:

| | |
|---|---|
| about 15 minutes | get it running and find your way around |
| about 20 minutes | find out what happened, from the data |
| about 35 minutes | one change, working |
| about 20 minutes | your notes |

---

## The setup

You're on the data team that owns member engagement. A daily pipeline rebuilds each
member's **engagement tier** in the warehouse and pushes that tier into the CRM.
Marketing build their campaigns on what the CRM holds, and some of their automation
fires on a tier **changing** rather than on what it currently is.

```
 upstream feeds  ──▶   warehouse   ──▶   dbt models   ──▶    CRM    ──▶   marketing
 membership,           (DuckDB)          engagement          member        automation
 engagement                              tiers               records
```

An Airflow DAG, `sync-member-engagement`, runs it every day at 02:00: it builds the
models, runs the tests tagged `critical`, then pushes every member's tier to the CRM.

There are four tiers, `champion`, `engaged`, `at_risk` and `dormant`, from most active to
least. How a member's tier is worked out is in `marts_engagement__member_tiers`. The tier
decides which campaign a member is in, and the one that matters below is `dormant`.

The code is in `dbt-project/` (the models and tests), `airflow-project/dags/` (the DAG
and its CRM client) and `scripts/` (loading the warehouse, running the pipeline without
Airflow, replaying the incident and resetting). Everything runs locally, against DuckDB
and a mock of the production CRM: there are no cloud credentials, nothing to spend, and
**nothing in the pipeline can reach a real service**.

The models take a `run_date`, so you can build them as they stood on any past run date,
and you can replay the incident and put everything back as many times as you like.
`SETUP.md` shows how.

---

## The incident

> **2026-09-25.** The daily `sync-member-engagement` pipeline pushed engagement tiers
> from the warehouse into the CRM, as it does every morning at 02:00.
>
> That run moved **708 of our ~12,000 members into the `dormant` tier**. Marketing's
> automation fires on the *transition into* `dormant`, so every one of them was sent the
> re-engagement email: the "we miss you, here's what you've missed" one.
>
> Nobody noticed. The pipeline ran again on the 26th, the 27th and the 28th, and
> **every run reported success.** By the 28th the automation had sent **732** of those
> emails in total.
>
> On the 28th a member replied to the email to say they had been using the platform
> that week, and please stop telling them they had gone quiet. That is how we found out:
> three days and 732 emails later.
>
> Marketing have asked us one thing: **do we need to do something about the people who
> got that email, or will it sort itself out once the pipeline is fixed?**

Today is **2026-09-29**. Nothing has been paused, and today's run hasn't gone yet: it's
the one you would be making.

---

## Your task

**Make this pipeline safe to run.**

That's the whole brief, and it's deliberately open. Part of what we're interested in is
how you decide what "safe" means here, what you do first, and what you consciously leave
alone.

**There is more wrong here than fits in 90 minutes, and we know that.** Don't try to
chase everything you spot: one thing done well and accounted for beats a list of
everything that might be wrong. If you notice other problems, put them in your notes,
unverified, and move on.

## What counts as finished

Stopping the pipeline, guarding it and changing it are all viable. Whichever you choose,
show us:

1. an unsafe behaviour **demonstrated**, not just suspected;
2. what you changed, working in the code, and what it doesn't cover;
3. what your choice costs (nothing here is free), and whether you'd be happy for the
   pipeline to run tonight;
4. an answer to marketing's question, and what it rests on.

We'll ask what you would fix next in the interview.

## If something is unclear

Anything about the business, such as how expensive another wrong email is, how long a
legitimate update can wait or who decides, has no answer in the repository. That's
deliberate. **Write down what you assumed and carry on**: it shows how you would have
moved without us, and we'll talk it through in the session. If something genuinely blocks
you, email us, but keep going rather than waiting for a reply.

Technical facts about how things work are in the repository:

| To find out | Look at |
|---|---|
| how to run everything | `SETUP.md` |
| what each upstream feed populates | `MIGRATION_MANIFEST.md` |
| what the CRM does, and what triggers marketing automation | `mock_crm/API.md` |
| what the source tables and the mart's columns mean | `dbt-project/models/_sources.yml`, `dbt-project/models/marts/_marts_engagement.yml` |
| DuckDB syntax you may not have used | `DUCKDB_REFERENCE.md` |

**Don't spend your 90 minutes reading.** There's a real warehouse here with real data in
it, and it's usually faster to ask the data a question than to work out what the code
would do.

## What to send back

Your code, plus **rough notes** in a `notes/` folder at the root: what you looked at, what
you concluded, what you considered and rejected, what you knowingly left undone, and what
you're unsure about. Bullets are fine. **We're assessing the reasoning, not the
presentation**, so don't write it up. Any number of files, in any format; a query you kept
is welcome.

**Send it back as a zip** of the folder, minus `.venv/` and `mock_data.duckdb`. **Keep
`.git/`** if you committed as you went: it saves you explaining the order you did things
in.
