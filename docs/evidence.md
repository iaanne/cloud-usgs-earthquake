# Bukti Eksekusi

Dokumen ini mencatat bukti yang harus diambil untuk laporan dan presentasi. Screenshot disimpan di folder `docs/img/`.

## 1. Yang perlu diambil

| No | Bukti | Cara mengambil | Status |
|---|---|---|---|
| 1 | Trigger run history dengan seluruh jendela Succeeded | ADF Studio, Monitor, Trigger runs | belum |
| 2 | Satu pipeline run detail, termasuk durasi dan data read/write | ADF Studio, Monitor, Pipeline runs, klik satu run | belum |
| 3 | Isi folder Bronze di portal storage, memperlihatkan partisi `year=` dan `dt=` | Portal storage, container `datalake`, folder `bronze/usgs_batch` | belum |
| 4 | Cuplikan 3 baris pertama CSV | `head -3` pada berkas hasil unduhan | belum |
| 5 | Jumlah berkas dan total ukuran | `scripts/verify_bronze.sh` | belum |
| 6 | Budget alert di Cost Management | Portal, Cost Management, Budgets | belum |
| 7 | Role assignment pada storage account | Portal, storage account, Access control (IAM) | belum |
| 8 | Metric `IncomingMessages` saat uji stream | Namespace Event Hubs, Metrics, rentang 14:15 sampai 14:52 UTC | sudah diambil sebagai angka, screenshot belum |
| 9 | Daftar berkas Avro di `bronze/usgs_stream` | `az storage fs file list` pada folder itu | sudah diverifikasi, 28 berkas. Ringkasan di `docs/img/streaming-summary.md` |
| 10 | Log eksekusi produser 10.000 event | Keluaran `producer/replay_bronze.py` dengan `--log` | sudah ada di `docs/replay_10k.log` |
| 11 | Contoh event hasil Capture | Baca kolom `Body` dari file Avro | sudah ada di `docs/img/contoh-event-capture.md` |

## 2. Angka yang sudah terverifikasi

| Metrik | Nilai | Sumber |
|---|---|---|
| Jendela trigger | 127 | Trigger runs ADF dan daftar blob |
| Jendela gagal | 0 | Trigger runs ADF |
| Berkas CSV | 127 | Blob list |
| Total baris | 300.230 | hitung baris seluruh CSV |
| Ukuran | 65,4 MB | Blob list |
| Uji satu jendela | 2.249 baris, 522 KB | run manual Januari 2024 |
| Jendela ekor | 1.300 baris, 296 KB | run manual 2026-10-17 |
| Event unik di Capture | 10.000 | hitung `id` unik dari 28 berkas Avro |
| Baris Avro total | 20.001 | 10.000 dari uji final, 10.001 dari uji pertama yang dihentikan |
| Durasi kirim final | 1.358 detik, 7,4 event per detik | log produser |

Catatan: run trigger semula menghasilkan 126 jendela karena jendela ke-127 melewati `endTime` 1 November 2026. Jendela ekor 17 Oktober sampai 1 November 2026 dijalankan manual dengan pipeline yang sama dan menulis ke `bronze/usgs_batch/year=2026/dt=2026-10-17/events.csv`.

## 3. Perintah verifikasi cepat

```bash
STORAGE=cloudearthquake CONTAINER=datalake ./scripts/verify_bronze.sh

RG=project-cloud-kelompok2 FACTORY=adf-cloudearthquake \
SINCE=2026-11-01T00:00:00Z UNTIL=2026-11-25T00:00:00Z \
./scripts/verify_bronze.sh
```
