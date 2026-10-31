"""
Retries queued emails from the consultation form:

- "we received your request" confirmations to the submitter
  (data/confirmation_queue.json), and
- new-lead notifications to RECIPIENT_EMAIL (data/lead_queue.json).

The /api/consult endpoint sends both immediately; if a send fails (mail
server down, etc.) it queues the email instead of dropping it. This
script is meant to run on a cron schedule (e.g. every 10-15 minutes) to
keep retrying until each one succeeds or ages out. See README.md for the
cron setup.

CLI only — it is not exposed over HTTP, so a random visitor can't
trigger it and burn through the queue's retry budget early.

Usage:  python -m backend.retry_confirmations
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime

from .lib import (
    CONFIRMATION_MAX_ATTEMPTS,
    CONFIRMATION_QUEUE,
    DATA_DIR,
    LEAD_MAX_ATTEMPTS,
    LEAD_QUEUE,
    append_line,
    clean,
    is_valid_email,
    send_confirmation,
    send_lead,
    update_json_list,
)


def retry_leads() -> dict[str, int]:
    counts = {"sent": 0, "dropped": 0, "remaining": 0}

    def process(queue: list) -> list:
        remaining = []
        for item in queue:
            if not isinstance(item, dict) or not item.get("subject") or not item.get("body"):
                continue
            subject, body, reply_to = str(item["subject"]), str(item["body"]), str(item.get("reply_to") or "")
            if send_lead(subject, body, reply_to):
                counts["sent"] += 1
                continue
            try:
                attempts = int(item.get("attempts") or 0) + 1
            except (TypeError, ValueError):
                attempts = 1
            if attempts >= LEAD_MAX_ATTEMPTS:
                # The lead itself is still in data/submissions.log.
                counts["dropped"] += 1
                append_line(
                    DATA_DIR / "lead_failures.log",
                    json.dumps(
                        {
                            "time": datetime.now().astimezone().isoformat(timespec="seconds"),
                            "subject": subject,
                            "reply_to": reply_to,
                            "attempts": attempts,
                        },
                        ensure_ascii=False,
                    ),
                )
                continue
            remaining.append({**item, "attempts": attempts})
        counts["remaining"] = len(remaining)
        return remaining

    update_json_list(LEAD_QUEUE, process)
    return counts


def main() -> int:
    try:
        leads = retry_leads()
    except OSError as exc:
        print(f"Could not open lead queue: {exc}", file=sys.stderr)
        return 1
    print(
        f"Lead retry: sent {leads['sent']}, gave up on {leads['dropped']}, "
        f"{leads['remaining']} still queued."
    )

    counts = {"sent": 0, "dropped": 0, "remaining": 0}

    def process(queue: list) -> list:
        remaining = []
        for item in queue:
            if not isinstance(item, dict):
                continue
            name = clean(str(item.get("name") or ""), 200)
            email = clean(str(item.get("email") or ""), 200)
            try:
                attempts = int(item.get("attempts") or 0)
            except (TypeError, ValueError):
                attempts = 0

            if name == "" or not is_valid_email(email):
                # Corrupt entry — nothing useful to retry.
                continue

            if send_confirmation(name, email):
                counts["sent"] += 1
                continue

            attempts += 1
            if attempts >= CONFIRMATION_MAX_ATTEMPTS:
                counts["dropped"] += 1
                append_line(
                    DATA_DIR / "confirmation_failures.log",
                    json.dumps(
                        {
                            "time": datetime.now().astimezone().isoformat(timespec="seconds"),
                            "name": name,
                            "email": email,
                            "attempts": attempts,
                        },
                        ensure_ascii=False,
                    ),
                )
                continue

            remaining.append(
                {
                    "name": name,
                    "email": email,
                    "attempts": attempts,
                    "queued_at": item.get("queued_at") or int(time.time()),
                }
            )
        counts["remaining"] = len(remaining)
        return remaining

    try:
        update_json_list(CONFIRMATION_QUEUE, process)
    except OSError as exc:
        print(f"Could not open confirmation queue: {exc}", file=sys.stderr)
        return 1

    print(
        f"Confirmation retry: sent {counts['sent']}, gave up on {counts['dropped']}, "
        f"{counts['remaining']} still queued."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
