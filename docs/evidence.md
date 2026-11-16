# Bukti Eksekusi

Dokumen ini mencatat bukti yang harus diambil untuk laporan dan presentasi. Screenshot disimpan di folder `docs/img/`.

## Yang perlu diambil

| No | Bukti | Cara mengambil | Status |
|---|---|---|---|
| 1 | Trigger run history dengan seluruh jendela Succeeded | ADF Studio, Monitor, Trigger runs | belum |
| 2 | Satu pipeline run detail, termasuk durasi dan data read/write | ADF Studio, Monitor, Pipeline runs, klik satu run | belum |
| 3 | Isi folder Bronze di portal storage, memperlihatkan partisi `year=` dan `dt=` | Portal storage, container `datalake`, folder `bronze/usgs_batch` | belum |
| 4 | Cuplikan 3 baris pertama CSV | `head -3` pada berkas hasil unduhan | belum |
| 5 | Jumlah berkas dan total ukuran | `scripts/verify_bronze.sh` | belum |
| 6 | Budget alert di Cost Management | Portal, Cost Management, Budgets | belum |
| 7 | Role assignment pada storage account | Portal, storage account, Access control (IAM) | belum |

## Angka yang sudah terverifikasi

| Metrik | Nilai | Sumber |
|---|---|---|
| Jendela trigger | 126 | Trigger runs ADF |
| Jendela gagal | 0 | Trigger runs ADF |
| Berkas CSV | 126 | Blob list |
| Total baris | 298.930 | hitung baris seluruh CSV |
| Ukuran | 65,1 MB | Blob list |
| Uji satu jendela | 2.249 baris, 522 KB | run manual Januari 2024 |

## Perintah verifikasi cepat

```bash
STORAGE=cloudearthquake CONTAINER=datalake ./scripts/verify_bronze.sh

RG=<resource-group> FACTORY=<nama-data-factory> \
SINCE=2026-11-16T00:00:00Z UNTIL=2026-11-18T00:00:00Z \
./scripts/verify_bronze.sh
```
