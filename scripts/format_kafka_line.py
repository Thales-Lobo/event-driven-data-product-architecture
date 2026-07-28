#!/usr/bin/env python3
"""Pretty-print raw Kafka console-consumer output into readable event blocks.

kafka-console-consumer.sh, run with `--property print.timestamp=true
--property print.key=true`, emits lines shaped like:

    CreateTime:1721600741931	base_industrial_production	{"event_type": ...}

That's accurate but hard to scan. This filter reads such lines from stdin and
re-renders each one as a short, human-readable block: a clock time, the
partition key, and the JSON payload pretty-printed -- while staying strictly
factual (no data is summarized or omitted, only reformatted).

Kept dependency-free (stdlib only) so it runs with whatever Python is already
on the host -- no extra install required beyond what the project already
needs (Docker + uv).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

RESET = "\033[0m"
DIM = "\033[2m"
BOLD = "\033[1m"
CYAN = "\033[36m"


def format_line(raw: str) -> str:
    """Convert one raw kafka-console-consumer line into a readable block."""
    raw = raw.rstrip("\n")
    if not raw.strip():
        return ""

    parts = raw.split("\t", 2)
    if len(parts) != 3 or not parts[0].startswith("CreateTime:"):
        # Unexpected shape (e.g. a broker warning banner) -- print as-is
        # rather than risk hiding information.
        return raw

    raw_ts, key, value = parts
    epoch_ms = int(raw_ts.removeprefix("CreateTime:"))
    ts = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")

    try:
        payload = json.loads(value)
        pretty_value = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False)
    except json.JSONDecodeError:
        pretty_value = value

    header = f"{DIM}{ts} UTC{RESET}  {BOLD}{CYAN}key={key}{RESET}"
    separator = f"{DIM}{'─' * 58}{RESET}"
    return f"{header}\n{pretty_value}\n{separator}"


def main() -> None:
    for line in sys.stdin:
        formatted = format_line(line)
        if formatted:
            print(formatted)
            sys.stdout.flush()


if __name__ == "__main__":
    main()