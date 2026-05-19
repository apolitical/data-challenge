"""Generate dense, realistic event data for the analytics engineer challenge.

Replaces raw_events.csv with 500-1000 events spanning March 3 - May 29, 2023.
Uses deterministic seed (42) for reproducibility.
"""

import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
USERS_CSV = PROJECT_ROOT / "mock_data" / "raw_users.csv"
COURSES_CSV = PROJECT_ROOT / "mock_data" / "raw_courses.csv"
ENROLMENTS_CSV = PROJECT_ROOT / "mock_data" / "raw_enrolments.csv"
OUTPUT_CSV = PROJECT_ROOT / "mock_data" / "raw_events.csv"

# Probability that an event for a user lands on one of the courses they're
# enrolled in (when they have at least one eligible enrolment). Reflects the
# fact that most platform activity happens on courses users have signed up to.
ENROLLED_BIAS = 0.85

# Date range
START_DATE = datetime(2023, 3, 3)
END_DATE = datetime(2023, 5, 29)
TOTAL_DAYS = (END_DATE - START_DATE).days + 1  # 88 days

# Event types (only the 4 specified types)
EVENT_TYPES = ["video_start", "video_complete", "quiz_start", "quiz_submit"]


def load_users():
    """Load non-deleted users with their signup dates."""
    users = []
    seen_ids = {}
    with open(USERS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = int(row["id"])
            deleted = row["deleted"].strip().upper()
            signup = datetime.strptime(row["signupDate"], "%Y-%m-%d")
            # Keep latest record per user (last seen wins for duplicate IDs)
            if deleted not in ("TRUE",):
                seen_ids[uid] = signup
    # Filter: only users whose signup is before or on END_DATE
    return {uid: signup for uid, signup in seen_ids.items() if signup <= END_DATE}


def load_courses():
    """Load courses that were available during the event window."""
    courses = []
    with open(COURSES_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            created = datetime.strptime(row["course_created_at"], "%Y-%m-%d")
            if created <= END_DATE:
                courses.append(int(row["course_id"]))
    return courses


def load_enrolments_by_user(eligible_courses):
    """Build a user_id -> list of enrolled, eligible course_ids lookup."""
    eligible_set = set(eligible_courses)
    by_user = {}
    with open(ENROLMENTS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = int(row["user_id"])
            cid = int(row["course_id"])
            if cid in eligible_set:
                by_user.setdefault(uid, []).append(cid)
    return by_user


def generate_session_id():
    """Random 6-char alphanumeric string."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def generate_metadata(event_type):
    """Generate metadata based on event type."""
    if event_type == "quiz_submit":
        return random.choice(["quiz:pass", "quiz:fail"])
    elif event_type == "quiz_start":
        return ""
    elif event_type in ("video_start", "video_complete"):
        return ""
    return ""


def main():
    users = load_users()
    courses = load_courses()
    enrolments_by_user = load_enrolments_by_user(courses)

    user_ids = sorted(users.keys())
    print(f"Non-deleted users: {len(user_ids)}")
    print(f"Courses: {len(courses)}")

    # Classify users into activity tiers
    random.shuffle(user_ids)
    n = len(user_ids)
    power_count = max(1, int(n * 0.10))
    casual_count = max(1, int(n * 0.50))
    # Rest are dormant

    power_users = set(user_ids[:power_count])
    casual_users = set(user_ids[power_count : power_count + casual_count])
    dormant_users = set(user_ids[power_count + casual_count :])

    print(f"Power users: {len(power_users)}, Casual: {len(casual_users)}, Dormant: {len(dormant_users)}")

    events = []
    event_id = 9001

    for day_offset in range(TOTAL_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        day_of_week = current_date.weekday()  # 0=Mon, 6=Sun
        is_weekend = day_of_week >= 5

        # Growth trend: slightly more events later in the range
        growth_factor = 1.0 + 0.3 * (day_offset / TOTAL_DAYS)

        # Base events per day target: ~10-15 avg => ~8 base weekday, ~4 weekend
        if is_weekend:
            base_events = 5
        else:
            base_events = 10

        target_events = int(base_events * growth_factor + random.gauss(0, 1.5))
        target_events = max(3, target_events)

        events_today = 0

        for _ in range(target_events):
            # Pick a user based on tier probabilities
            # Power users: high chance each day
            # Casual users: moderate chance
            # Dormant users: low chance
            roll = random.random()
            if roll < 0.40:
                pool = list(power_users)
            elif roll < 0.85:
                pool = list(casual_users)
            else:
                pool = list(dormant_users)

            if not pool:
                pool = list(casual_users) or list(power_users)

            uid = random.choice(pool)
            signup_date = users[uid]

            # Skip if user hasn't signed up yet
            if current_date < signup_date:
                continue

            # Prefer courses the user is enrolled in, with occasional drift
            # to other courses to reflect browsing/preview behaviour.
            user_enrolled = enrolments_by_user.get(uid)
            if user_enrolled and random.random() < ENROLLED_BIAS:
                course_id = random.choice(user_enrolled)
            else:
                course_id = random.choice(courses)
            event_type = random.choice(EVENT_TYPES)

            # Random time during the day (biased toward working hours)
            hour = int(random.gauss(14, 4)) % 24
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            timestamp = current_date.replace(hour=hour, minute=minute, second=second)

            session_id = generate_session_id()
            metadata = generate_metadata(event_type)

            events.append({
                "id": event_id,
                "user_id": uid,
                "course_id": course_id,
                "event_type": event_type,
                "event_timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "session_id": session_id,
                "metadata": metadata,
            })
            event_id += 1
            events_today += 1

    # Write CSV
    fieldnames = ["id", "user_id", "course_id", "event_type", "event_timestamp", "session_id", "metadata"]
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    # Stats
    total = len(events)
    unique_users = len({e["user_id"] for e in events})
    avg_per_day = total / TOTAL_DAYS

    # Per-day breakdown
    from collections import Counter
    day_counts = Counter()
    for e in events:
        day_counts[e["event_timestamp"][:10]] += 1

    weekday_events = sum(
        v for k, v in day_counts.items()
        if datetime.strptime(k, "%Y-%m-%d").weekday() < 5
    )
    weekend_events = sum(
        v for k, v in day_counts.items()
        if datetime.strptime(k, "%Y-%m-%d").weekday() >= 5
    )
    weekdays = sum(1 for k in day_counts if datetime.strptime(k, "%Y-%m-%d").weekday() < 5)
    weekends = sum(1 for k in day_counts if datetime.strptime(k, "%Y-%m-%d").weekday() >= 5)

    print(f"\n--- Generated Events Stats ---")
    print(f"Total events: {total}")
    print(f"Event ID range: 9001-{9000 + total}")
    print(f"Average events/day: {avg_per_day:.1f}")
    print(f"Weekday avg: {weekday_events / max(weekdays, 1):.1f} events/day ({weekdays} weekdays)")
    print(f"Weekend avg: {weekend_events / max(weekends, 1):.1f} events/day ({weekends} weekend days)")
    print(f"Unique users with events: {unique_users}")
    print(f"Date range: {min(e['event_timestamp'] for e in events)[:10]} to {max(e['event_timestamp'] for e in events)[:10]}")
    print(f"Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
