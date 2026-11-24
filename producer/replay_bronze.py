#!/usr/bin/env python3
"""Putar ulang CSV Bronze menjadi event JSON ke Event Hub.

Skrip ini membaca satu atau beberapa berkas CSV hasil ingest ADF, mengubah
setiap baris menjadi event sesuai kontrak di docs/event-schema.md, lalu
mengirimnya ke Event Hub dengan partition key berupa id kejadian.

Connection string tidak disimpan di repo. Nilainya dibaca saat runtime dari
Azure Key Vault memakai identitas pemanggil (DefaultAzureCredential).

Contoh:
    python producer/replay_bronze.py \
        --source data/bronze \
        --limit 10000 \
        --vault-url https://kv-earthquake-l0224036.vault.azure.net/ \
        --secret-name eventhub-usgs-send \
        --eventhub-name usgs-events
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Iterator

from azure.core.exceptions import ServiceRequestError
from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

LOG = logging.getLogger("replay_bronze")

# Kolom CSV Bronze yang dipakai, sesuai docs/event-schema.md.
NUMERIC_FIELDS = ("latitude", "longitude", "depth", "mag")
TEXT_FIELDS = ("id", "time", "updated", "magType", "place", "type")
RENAME = {"depth": "depth_km", "magType": "mag_type"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", action="append", required=True,
                   help="Berkas CSV, direktori, atau pola glob. Boleh diulang.")
    p.add_argument("--limit", type=int, default=0,
                   help="Jumlah event maksimum. 0 berarti semua baris.")
    p.add_argument("--batch-size", type=int, default=100,
                   help="Jumlah event per putaran kirim. Default 100.")
    p.add_argument("--rate", type=float, default=200.0,
                   help="Batas laju event per detik. Default 200.")
    p.add_argument("--retries", type=int, default=3,
                   help="Percobaan ulang per event saat gagal. Default 3.")
    p.add_argument("--workers", type=int, default=32,
                   help="Jumlah pengiriman paralel per kelompok. Default 32.")
    p.add_argument("--vault-url", default=os.environ.get("KEY_VAULT_URL"),
                   help="URL Key Vault. Bisa juga dari KEY_VAULT_URL.")
    p.add_argument("--secret-name", default=os.environ.get("EVENTHUB_SECRET_NAME", "eventhub-usgs-send"),
                   help="Nama secret berisi connection string Event Hub.")
    p.add_argument("--eventhub-name", default=os.environ.get("EVENTHUB_NAME", "usgs-events"),
                   help="Nama Event Hub tujuan.")
    p.add_argument("--dry-run", action="store_true",
                   help="Hanya baca dan ubah ke JSON, tanpa mengirim.")
    p.add_argument("--log", default=None, help="Berkas log. Kosong berarti hanya ke layar.")
    return p.parse_args(argv)


def setup_logging(path: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if path:
        handlers.append(logging.FileHandler(path, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(message)s")


def expand_sources(patterns: Iterable[str]) -> list[str]:
    files: list[str] = []
    for item in patterns:
        if os.path.isdir(item):
            files.extend(sorted(glob.glob(os.path.join(item, "**", "*.csv"), recursive=True)))
        elif os.path.isfile(item):
            files.append(item)
        else:
            files.extend(sorted(glob.glob(item, recursive=True)))
    return sorted(dict.fromkeys(files))


def to_number(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def to_text(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def row_to_event(row: dict[str, str]) -> dict:
    event: dict = {}
    for field in TEXT_FIELDS:
        event[RENAME.get(field, field)] = to_text(row.get(field))
    for field in NUMERIC_FIELDS:
        event[RENAME.get(field, field)] = to_number(row.get(field))
    return {key: event[key] for key in
            ("id", "time", "updated", "latitude", "longitude",
             "depth_km", "mag", "mag_type", "place", "type")}


def read_events(files: list[str], limit: int) -> Iterator[dict]:
    sent = 0
    for path in files:
        with open(path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                event = row_to_event(row)
                if not event.get("id"):
                    continue
                yield event
                sent += 1
                if limit and sent >= limit:
                    return


def read_connection_string(vault_url: str, secret_name: str) -> str:
    client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
    secret = client.get_secret(secret_name)
    if not secret.value:
        raise RuntimeError(f"secret {secret_name} kosong")
    return secret.value


def send_with_retry(producer: EventHubProducerClient, event: dict, retries: int) -> None:
    payload = json.dumps(event, ensure_ascii=False)
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            producer.send_event(EventData(payload), partition_key=str(event["id"]))
            return
        except (EventHubError, ServiceRequestError, OSError) as exc:
            if attempt == retries:
                raise
            LOG.warning("gagal kirim id=%s (percobaan %d/%d): %s",
                        event.get("id"), attempt, retries, exc)
            time.sleep(delay)
            delay *= 2


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log)

    files = expand_sources(args.source)
    if not files:
        LOG.error("tidak ada berkas CSV yang cocok dengan %s", args.source)
        return 2
    LOG.info("%d berkas sumber, limit %s event", len(files), args.limit or "tanpa batas")

    producer = None
    if not args.dry_run:
        if not args.vault_url:
            LOG.error("--vault-url atau KEY_VAULT_URL wajib diisi")
            return 2
        connection_string = read_connection_string(args.vault_url, args.secret_name)
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string, eventhub_name=args.eventhub_name)
        LOG.info("terhubung ke Event Hub %s", args.eventhub_name)

    started = time.monotonic()
    sent = 0
    failed = 0
    group: list[dict] = []
    pool = None if args.dry_run else ThreadPoolExecutor(max_workers=args.workers)

    def send_one(event: dict) -> bool:
        try:
            send_with_retry(producer, event, args.retries)
            return True
        except Exception as exc:  # noqa: BLE001 - dicatat lalu dilanjutkan
            LOG.error("gagal permanen id=%s: %s", event.get("id"), exc)
            return False

    def flush(batch: list[dict]) -> tuple[int, int]:
        if args.dry_run:
            for event in batch:
                json.dumps(event, ensure_ascii=False)
            return len(batch), 0
        results = list(pool.map(send_one, batch))
        ok = sum(1 for item in results if item)
        return ok, len(batch) - ok

    def pace(total_ok: int) -> None:
        """Tahan laju kumulatif pada batas --rate event per detik."""
        target = total_ok / args.rate
        elapsed = time.monotonic() - started
        if target > elapsed:
            time.sleep(target - elapsed)

    try:
        for event in read_events(files, args.limit):
            group.append(event)
            if len(group) >= args.batch_size:
                ok, bad = flush(group)
                sent += ok
                failed += bad
                group = []
                pace(sent)
                LOG.info("terkirim %d, gagal %d", sent, failed)
        if group:
            ok, bad = flush(group)
            sent += ok
            failed += bad
            pace(sent)
    finally:
        if pool is not None:
            pool.shutdown(wait=True)
        if producer is not None:
            producer.close()

    duration = time.monotonic() - started
    rate = sent / duration if duration else 0.0
    LOG.info("selesai: %d terkirim, %d gagal, %.1f detik, %.1f event/detik",
             sent, failed, duration, rate)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
