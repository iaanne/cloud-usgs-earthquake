#!/usr/bin/env bash
# Verifikasi hasil ingest Bronze.
# Pemakaian:
#   STORAGE=cloudearthquake CONTAINER=datalake ./scripts/verify_bronze.sh

set -euo pipefail

STORAGE="${STORAGE:-cloudearthquake}"
CONTAINER="${CONTAINER:-datalake}"
PREFIX="${PREFIX:-bronze/usgs_batch/}"

echo "== Jumlah berkas CSV dan total ukuran =="
az storage blob list \
  --account-name "$STORAGE" --auth-mode login -c "$CONTAINER" --prefix "$PREFIX" \
  --query "[?ends_with(name,'events.csv')]" -o json |
python3 -c '
import json, sys
v = json.load(sys.stdin)
print("berkas :", len(v))
print("ukuran :", round(sum(x["properties"]["contentLength"] for x in v) / 1048576, 1), "MB")
'

echo
echo "== Jendela trigger =="
az datafactory trigger-run query-by-factory \
  --resource-group "${RG:?set RG}" --factory-name "${FACTORY:?set FACTORY}" \
  --last-updated-after "${SINCE:?set SINCE, contoh 2026-01-01T00:00:00Z}" \
  --last-updated-before "${UNTIL:?set UNTIL}" -o json |
python3 -c '
import json, sys, collections
v = json.load(sys.stdin).get("value", [])
print("jendela:", len(v), dict(collections.Counter(x["status"] for x in v)))
'

echo
echo "== Catatan =="
echo "Jumlah baris data = total baris seluruh CSV dikurangi satu baris header per berkas."
echo "Perkiraan: ukuran MB dibagi 189 byte per baris."
