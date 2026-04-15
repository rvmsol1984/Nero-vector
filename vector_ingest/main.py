"""Entry point for the NERO Vector UAL ingest service.

Runs migrations, builds a TenantIngestor per tenant from tenants.json,
and polls every VECTOR_POLL_INTERVAL seconds (default 300 = 5 minutes)
until the process receives SIGINT/SIGTERM.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

from pythonjsonlogger import jsonlogger

from vector_ingest.db import Database
from vector_ingest.defender_ingest import DefenderIngestor
from vector_ingest.ingestor import TenantIngestor
from vector_ingest.ioc_enricher import IocEnricher
from vector_ingest.message_trace import MessageTraceIngestor
from vector_ingest.signin_logs import SignInLogPoller
from vector_ingest.threatlocker_ingest import ThreatLockerIngestor
from vector_ingest.scoring_engine import ScoringEngine, BaselineEngine


def configure_logging() -> None:
    level_name = os.environ.get("VECTOR_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"asctime": "ts", "levelname": "level", "name": "logger"},
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


logger = logging.getLogger("vector_ingest.main")


_SHUTDOWN = False


def _handle_signal(signum, _frame) -> None:
    global _SHUTDOWN
    logger.info("shutdown signal received", extra={"signal": signum})
    _SHUTDOWN = True


def load_tenants(path: str | Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("tenants.json must be a JSON array")
    for entry in data:
        if "tenant_id" not in entry or "name" not in entry:
            raise ValueError("tenants.json entries require 'name' and 'tenant_id'")
    return data


def build_ingestors(tenants: list[dict], db: Database) -> list:
    """Build a flat list of pollers: one TenantIngestor per tenant plus
    one DefenderIngestor for every tenant whose ``license_tier`` is
    ``E5`` (Defender requires the advanced hunting SKU).

    Both types expose a ``poll_once()`` method so the main loop can
    iterate uniformly.
    """
    client_id = os.environ.get("VECTOR_CLIENT_ID")
    client_secret = os.environ.get("VECTOR_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "VECTOR_CLIENT_ID and VECTOR_CLIENT_SECRET must be set in the environment"
        )

    ingestors: list = []
    for t in tenants:
        ingestors.append(
            TenantIngestor(
                tenant_id=t["tenant_id"],
                client_name=t["name"],
                client_id=client_id,
                client_secret=client_secret,
                db=db,
            )
        )
        ingestors.append(
            MessageTraceIngestor(
                tenant_id=t["tenant_id"],
                client_name=t["name"],
                client_id=client_id,
                client_secret=client_secret,
                db=db,
                license_tier=t.get("license_tier", "BizPremium"),
            )
        )
        # Graph /auditLogs/signIns is available to every tenant that
        # consents to AuditLog.Read.All, regardless of license tier.
        # The poller self-disables on the first 403 so we don't have
        # to gate construction here -- every tenant gets one.
        ingestors.append(
            SignInLogPoller(
                tenant_id=t["tenant_id"],
                client_name=t["name"],
                client_id=client_id,
                client_secret=client_secret,
                db=db,
            )
        )
        if str(t.get("license_tier", "")).upper() == "E5":
            logger.info(
                "[defender] building ingestor for E5 tenant",
                extra={"tenant_id": t["tenant_id"], "client_name": t["name"]},
            )
            ingestors.append(
                DefenderIngestor(
                    tenant_id=t["tenant_id"],
                    client_name=t["name"],
                    client_id=client_id,
                    client_secret=client_secret,
                    db=db,
                )
            )

    # Global workers -- not per-tenant but share the same poll cadence.
    # ThreatLocker is a separate SaaS outside Azure AD so it's wired
    # off a dedicated env token rather than the tenants.json file.
    # Skipping entirely (not even constructing the ingestor) keeps
    # installs that don't have ThreatLocker from logging 401 warnings
    # every 5 minutes.
    tl_token = os.environ.get("THREATLOCKER_API_TOKEN", "").strip()
    tl_org_id = os.environ.get("THREATLOCKER_ORG_ID", "").strip()
    tl_client_name = os.environ.get(
        "THREATLOCKER_CLIENT_NAME", "GameChange Solar"
    ).strip() or "GameChange Solar"
    if tl_token and tl_org_id:
        logger.info(
            "[threatlocker] building ingestor",
            extra={"org_id": tl_org_id, "client_name": tl_client_name},
        )
        ingestors.append(
            ThreatLockerIngestor(
                tenant_id=tl_org_id,
                client_name=tl_client_name,
                api_token=tl_token,
                db=db,
            )
        )
    else:
        logger.info(
            "[threatlocker] token/org not set, skipping ingestor",
            extra={"has_token": bool(tl_token), "has_org_id": bool(tl_org_id)},
        )

    # ScoringEngine and BaselineEngine
    ingestors.append(BaselineEngine(db=db))
    ingestors.append(ScoringEngine(db=db))

    # IocEnricher runs once per cycle and walks every tenant's recent
    # events looking for OpenCTI-backed indicator matches. It's last in
    # the list so it runs after the other ingestors have committed any
    # new rows this cycle.
    ingestors.append(IocEnricher(db=db))
    return ingestors


def main() -> int:
    configure_logging()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    tenants_file = os.environ.get("VECTOR_TENANTS_FILE", "/app/tenants.json")
    poll_interval = int(os.environ.get("VECTOR_POLL_INTERVAL", "300"))

    logger.info(
        "vector-ingest starting",
        extra={"tenants_file": tenants_file, "poll_interval": poll_interval},
    )

    tenants = load_tenants(tenants_file)
    logger.info("loaded tenants", extra={"count": len(tenants)})

    db = Database()
    db.connect()

    migrations_dir = os.environ.get("VECTOR_MIGRATIONS_DIR", "/app/migrations")
    db.run_migrations(migrations_dir)

    ingestors = build_ingestors(tenants, db)

    try:
        while not _SHUTDOWN:
            cycle_start = time.monotonic()
            for ingestor in ingestors:
                if _SHUTDOWN:
                    break
                kind = type(ingestor).__name__
                logger.info(
                    "polling ingestor",
                    extra={
                        "kind": kind,
                        "tenant_id": ingestor.tenant_id,
                        "client_name": ingestor.client_name,
                    },
                )
                try:
                    ingestor.poll_once()
                except Exception as exc:
                    logger.exception(
                        "ingestor poll crashed",
                        extra={
                            "kind": kind,
                            "tenant_id": ingestor.tenant_id,
                            "client_name": ingestor.client_name,
                            "error": str(exc),
                        },
                    )

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(5, poll_interval - int(elapsed))
            logger.info(
                "cycle complete",
                extra={"elapsed_sec": round(elapsed, 2), "sleep_sec": sleep_for},
            )
            # Sleep in 1-second slices so signals are honored promptly.
            for _ in range(sleep_for):
                if _SHUTDOWN:
                    break
                time.sleep(1)
    finally:
        db.close()
        logger.info("vector-ingest stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
