"""End-to-end demo driver.

Sequentially triggers the heterogeneous connectors through the API and prints the
resulting GDP after the orchestrator reacts. Run from the host once the stack is
up: ``uv run python scripts/trigger_demo.py``.
"""

from __future__ import annotations

import time

import httpx

API = "http://localhost:8000"


def _ingest(connector: str) -> None:
    resp = httpx.post(f"{API}/ingest/{connector}", timeout=30)
    resp.raise_for_status()
    event = resp.json()
    print(f"  ingested {event['base_data_id']} v{event['version']} = {event['value']}")


def _print_gdp(label: str) -> None:
    resp = httpx.get(f"{API}/data-products/monthly_gdp", timeout=30)
    if resp.status_code == 404:
        print(f"  [{label}] GDP not computed yet")
        return
    data = resp.json()
    lineage = ", ".join(
        f"{e['base_data_id']}@v{e['version']}" for e in data["composition_lineage"]
    )
    print(f"  [{label}] GDP v{data['version']} = {data['value']}  (lineage: {lineage})")


def main() -> None:
    print("1) Ingest industrial production (CSV) + services revenue (TXT)")
    _ingest("csv_industrial")
    _ingest("txt_services")
    time.sleep(2)  # allow the orchestrator to react
    _print_gdp("after first ingest")

    print("\n2) New industrial reading arrives via the mock API -> auto recalculation")
    _ingest("api_industrial")
    time.sleep(2)
    _print_gdp("after API update")

    print("\n3) Full auditable history of the GDP indicator")
    hist = httpx.get(f"{API}/data-products/monthly_gdp/history", timeout=30).json()
    for row in hist:
        print(f"  v{row['version']} = {row['value']} @ {row['created_at']}")


if __name__ == "__main__":
    main()