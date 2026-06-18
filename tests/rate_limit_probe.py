"""
Standalone rate-limit / throttling probe for the Power BI REST endpoints
this project uses.

It empirically *limit-tests* each endpoint by issuing a controlled burst of
requests and recording everything useful for troubleshooting throttling:

  * per-request HTTP status + latency
  * the request number at which the FIRST HTTP 429 (Too Many Requests) appears
  * the exact ``Retry-After`` value the API returns
  * every rate-limit-related response header
    (``Retry-After``, ``RateLimit-*``, ``X-RateLimit-*``, ``x-ms-*``)

Endpoints covered (only the ones this project calls):
  1. GET  /groups                                  (discovery)
  2. GET  /groups/{ws}/datasets/{ds}/refreshes     (refresh history)
  3. GET  /admin/activityevents                    (usage - admin API, tight quota)
  4. POST /groups/{ws}/datasets/{ds}/executeQueries(response-time probe)

It writes a JSON report (``rate_limit_report.json`` by default) plus a readable
console summary.

This is NOT a pytest test - it is a manual diagnostic you run against the live
tenant with the service principal credentials. Credentials are read from
ENVIRONMENT VARIABLES so nothing is ever written to disk or committed.

SAFETY
------
The admin ``activityevents`` API shares a tight, tenant-wide quota
(~200 requests/hour). Discovering its limit can temporarily exhaust that quota
for everyone in the tenant, so:
  * the admin endpoint is OPT-IN (``--include-admin``) and capped low by default,
  * every endpoint is capped by ``--max-requests`` (default 50),
  * the probe STOPS hitting an endpoint as soon as it sees a 429 (it has found
    the limit; hammering further only wastes quota).

USAGE
-----
1. Install deps (one-time):
       pip install msal requests

2. Set credentials as env vars (do NOT paste them into this file):

   macOS / Linux:
       export PBI_TENANT_ID="..."
       export PBI_CLIENT_ID="..."
       export PBI_CLIENT_SECRET="the-secret-VALUE-not-the-secret-id"

   Windows PowerShell:
       $env:PBI_TENANT_ID="..."; $env:PBI_CLIENT_ID="..."; $env:PBI_CLIENT_SECRET="..."

   Optional (enables the refresh-history and executeQueries probes):
       export PBI_WORKSPACE_ID="<workspace/group id>"
       export PBI_DATASET_ID="<dataset/semantic model id>"

3. Run it:
       # safe default - discovery + refresh + executeQueries, 50 reqs each
       python tests/rate_limit_probe.py

       # also probe the admin activityevents quota (uses tenant-wide quota!)
       python tests/rate_limit_probe.py --include-admin --max-requests 30

       # gentler pacing between requests
       python tests/rate_limit_probe.py --delay 0.25
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import msal
import requests

AUTHORITY_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
BASE_URL = "https://api.powerbi.com/v1.0/myorg"

# Response headers that carry throttling / quota information across the various
# Microsoft + RFC conventions. Matched case-insensitively.
RATE_LIMIT_HEADER_PREFIXES = ("ratelimit-", "x-ratelimit-", "x-ms-")
RATE_LIMIT_HEADER_EXACT = ("retry-after",)


def get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=AUTHORITY_TEMPLATE.format(tenant_id=tenant_id),
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        err = result.get("error_description", result.get("error", "unknown error"))
        raise SystemExit(f"\n[FAIL] Token acquisition failed:\n{err}\n")
    return result["access_token"]


def extract_rate_limit_headers(headers: requests.structures.CaseInsensitiveDict) -> dict[str, str]:
    """Return only the throttling-relevant response headers."""
    found: dict[str, str] = {}
    for name, value in headers.items():
        low = name.lower()
        if low in RATE_LIMIT_HEADER_EXACT or low.startswith(RATE_LIMIT_HEADER_PREFIXES):
            found[name] = value
    return found


def probe_endpoint(
    name: str,
    do_request: Callable[[], requests.Response],
    max_requests: int,
    delay: float,
) -> dict[str, Any]:
    """Burst-call one endpoint until a 429 appears or max_requests is reached.

    Returns a structured result with the throttling evidence.
    """
    print(f"\n=== Probing: {name} ===")
    requests_made = 0
    first_429_at: int | None = None
    retry_after: str | None = None
    rate_limit_headers: dict[str, str] = {}
    latencies_ms: list[float] = []
    statuses: list[int] = []
    error: str | None = None

    for i in range(1, max_requests + 1):
        try:
            start = time.perf_counter()
            resp = do_request()
            elapsed_ms = (time.perf_counter() - start) * 1000.0
        except requests.RequestException as exc:
            error = f"request #{i} raised: {exc}"
            print(f"  request #{i}: ERROR {exc}")
            break

        requests_made = i
        latencies_ms.append(round(elapsed_ms, 1))
        statuses.append(resp.status_code)
        headers = extract_rate_limit_headers(resp.headers)

        if i == 1 and headers:
            # Some tenants advertise the quota on every response - capture it.
            rate_limit_headers = dict(headers)

        if resp.status_code == 429:
            first_429_at = i
            retry_after = resp.headers.get("Retry-After")
            rate_limit_headers = dict(headers)
            print(
                f"  request #{i}: 429 THROTTLED  "
                f"Retry-After={retry_after}  ({elapsed_ms:.0f} ms)"
            )
            print(f"    rate-limit headers: {json.dumps(rate_limit_headers)}")
            break

        if resp.status_code in (401, 403):
            error = (
                f"HTTP {resp.status_code} - authenticated but not authorized for this "
                f"endpoint (check tenant setting / workspace access)."
            )
            print(f"  request #{i}: {resp.status_code} - {error}")
            break

        if resp.status_code >= 400:
            error = f"HTTP {resp.status_code}: {resp.text[:200]}"
            print(f"  request #{i}: {resp.status_code} (stopping) {resp.text[:120]}")
            break

        if i == 1 or i % 10 == 0:
            print(f"  request #{i}: {resp.status_code}  ({elapsed_ms:.0f} ms)")

        time.sleep(delay)

    throttled = first_429_at is not None
    result = {
        "endpoint": name,
        "requests_made": requests_made,
        "throttled": throttled,
        "first_429_at_request": first_429_at,
        "retry_after_seconds": retry_after,
        "rate_limit_headers": rate_limit_headers,
        "min_latency_ms": min(latencies_ms) if latencies_ms else None,
        "max_latency_ms": max(latencies_ms) if latencies_ms else None,
        "avg_latency_ms": round(sum(latencies_ms) / len(latencies_ms), 1) if latencies_ms else None,
        "status_counts": {str(s): statuses.count(s) for s in sorted(set(statuses))},
        "error": error,
    }

    if throttled:
        print(
            f"  -> Hit the limit after {first_429_at} request(s). "
            f"Retry-After={retry_after}s."
        )
    elif error:
        print(f"  -> Stopped early: {error}")
    else:
        print(
            f"  -> Sent {requests_made} request(s) with NO 429 "
            f"(limit not reached at this volume)."
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Power BI API rate-limit probe")
    parser.add_argument(
        "--max-requests",
        type=int,
        default=50,
        help="Max requests per endpoint before giving up looking for a 429 (default 50).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Seconds to sleep between requests (default 0 = burst as fast as possible).",
    )
    parser.add_argument(
        "--include-admin",
        action="store_true",
        help="Also probe /admin/activityevents (uses the tenant-wide admin quota!).",
    )
    parser.add_argument(
        "--output",
        default="rate_limit_report.json",
        help="Path to write the JSON report (default rate_limit_report.json).",
    )
    args = parser.parse_args()

    tenant_id = os.environ.get("PBI_TENANT_ID")
    client_id = os.environ.get("PBI_CLIENT_ID")
    client_secret = os.environ.get("PBI_CLIENT_SECRET")
    workspace_id = os.environ.get("PBI_WORKSPACE_ID")
    dataset_id = os.environ.get("PBI_DATASET_ID")

    missing = [
        n
        for n, v in [
            ("PBI_TENANT_ID", tenant_id),
            ("PBI_CLIENT_ID", client_id),
            ("PBI_CLIENT_SECRET", client_secret),
        ]
        if not v
    ]
    if missing:
        raise SystemExit(
            f"\n[FAIL] Missing env vars: {', '.join(missing)}.\n"
            "See the USAGE section at the top of this file.\n"
        )

    print("Acquiring access token as the service principal...")
    token = get_token(tenant_id, client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print("  [OK] Token acquired.")

    admin_max = min(args.max_requests, 30)  # keep admin probing conservative
    results: list[dict[str, Any]] = []

    # 1. Discovery - GET /groups
    results.append(
        probe_endpoint(
            "GET /groups (discovery)",
            lambda: requests.get(f"{BASE_URL}/groups", headers=headers, timeout=30),
            args.max_requests,
            args.delay,
        )
    )

    # 2. Refresh history (needs workspace + dataset IDs)
    if workspace_id and dataset_id:
        url = f"{BASE_URL}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        results.append(
            probe_endpoint(
                "GET refreshes (refresh history)",
                lambda: requests.get(url, headers=headers, params={"$top": 1}, timeout=30),
                args.max_requests,
                args.delay,
            )
        )

        # 4. executeQueries (POST) - response-time probe path
        eq_url = f"{BASE_URL}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        eq_body = {
            "queries": [{"query": 'EVALUATE ROW("x", 1)'}],
            "serializerSettings": {"includeNulls": True},
        }
        results.append(
            probe_endpoint(
                "POST executeQueries (response-time probe)",
                lambda: requests.post(eq_url, headers=headers, json=eq_body, timeout=30),
                args.max_requests,
                args.delay,
            )
        )
    else:
        print(
            "\n[SKIP] refresh-history + executeQueries probes "
            "(set PBI_WORKSPACE_ID and PBI_DATASET_ID to include them)."
        )

    # 3. Admin activityevents - opt-in, conservative
    if args.include_admin:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
        ae_url = f"{BASE_URL}/admin/activityevents"
        ae_params = {
            "startDateTime": f"'{yesterday}T00:00:00'",
            "endDateTime": f"'{yesterday}T23:59:59'",
        }
        results.append(
            probe_endpoint(
                "GET /admin/activityevents (usage - admin quota)",
                lambda: requests.get(ae_url, headers=headers, params=ae_params, timeout=30),
                admin_max,
                max(args.delay, 0.2),
            )
        )
    else:
        print(
            "\n[SKIP] /admin/activityevents probe "
            "(pass --include-admin to test it; it uses the tenant-wide admin quota)."
        )

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "max_requests": args.max_requests,
            "delay_seconds": args.delay,
            "include_admin": args.include_admin,
        },
        "results": results,
    }
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    print("\n" + "=" * 64)
    print("  RATE-LIMIT PROBE SUMMARY")
    print("=" * 64)
    for r in results:
        verdict = (
            f"throttled after {r['first_429_at_request']} req "
            f"(Retry-After={r['retry_after_seconds']}s)"
            if r["throttled"]
            else (r["error"] or f"no 429 in {r['requests_made']} req")
        )
        print(f"  - {r['endpoint']}: {verdict}")
    print("=" * 64)
    print(f"\nFull report written to: {args.output}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
