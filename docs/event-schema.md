# Skema Event Stream

Dokumen ini adalah kontrak antara bagian ingest batch (Ian) dan bagian Silver, Gold, serta uji Function (Elias dan Kesan). Produser membaca CSV di Bronze, mengubah setiap baris menjadi satu event JSON, lalu mengirimnya ke Event Hub `usgs-events`.

## 1. Sepuluh field wajib

| Field | Tipe JSON | Satuan | Kolom Bronze | Catatan |
|---|---|---|---|---|
| `id` | string | - | `id` | Kunci unik kejadian dari USGS, dipakai juga sebagai partition key |
| `time` | string | ISO 8601 UTC | `time` | Waktu kejadian, contoh `2026-10-16T23:57:50.582Z` |
| `updated` | string | ISO 8601 UTC | `updated` | Waktu versi terakhir katalog, dipakai memilih versi terbaru saat deduplikasi |
| `latitude` | number | derajat | `latitude` | Rentang -90 sampai 90 |
| `longitude` | number | derajat | `longitude` | Rentang -180 sampai 180 |
| `depth_km` | number atau null | kilometer | `depth` | Nama field diubah dari `depth` menjadi `depth_km` |
| `mag` | number atau null | skala magnitudo | `mag` | Filter sumber minimal 2,5 |
| `mag_type` | string atau null | - | `magType` | Nama field diubah dari `magType` menjadi `mag_type` |
| `place` | string atau null | - | `place` | Deskripsi lokasi dari USGS |
| `type` | string | - | `type` | Jenis kejadian, tidak selalu `earthquake` |

Aturan konversi:

1. Angka dikirim sebagai number, bukan string. Field kosong di CSV dikirim sebagai `null`, bukan string kosong.
2. Nama field memakai `snake_case`. Hanya `depth_km` dan `mag_type` yang berubah nama, sisanya sama dengan kolom sumber.
3. Tidak ada field tambahan di luar sepuluh field ini. Metadata pengiriman, seperti waktu kirim dan nomor partisi, tidak dimasukkan ke payload.
4. `id` dipakai sebagai partition key, sehingga seluruh versi satu kejadian masuk ke partisi yang sama dan urutannya terjaga.

## 2. Tiga contoh baris asli dari Bronze

Ketiga baris ini diambil apa adanya dari `bronze/usgs_batch/year=2026/dt=2026-09-17/events.csv`, lalu diubah sesuai aturan di atas.

```json
{
  "id": "us6000u2ww",
  "time": "2026-10-16T23:57:50.582Z",
  "updated": "2026-11-03T05:57:08.665Z",
  "latitude": 38.2992,
  "longitude": 20.6656,
  "depth_km": 10.0,
  "mag": 4.3,
  "mag_type": "mwr",
  "place": "5 km NNE of Sámi, Greece",
  "type": "earthquake"
}
```

```json
{
  "id": "us6000u2wr",
  "time": "2026-10-16T23:35:03.465Z",
  "updated": "2026-11-03T05:49:26.040Z",
  "latitude": 32.381,
  "longitude": -104.4685,
  "depth_km": 5.767,
  "mag": 2.5,
  "mag_type": "ml",
  "place": "22 km W of Carlsbad, New Mexico",
  "type": "earthquake"
}
```

```json
{
  "id": "us6000u3xe",
  "time": "2026-10-16T23:24:43.650Z",
  "updated": "2026-11-02T06:10:48.040Z",
  "latitude": 51.6813,
  "longitude": -173.7808,
  "depth_km": 24.875,
  "mag": 3.0,
  "mag_type": "ml",
  "place": "64 km SSE of Atka, Alaska",
  "type": "earthquake"
}
```

Baris asli di CSV untuk contoh pertama:

```csv
time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontalError,depthError,magError,magNst,status,locationSource,magSource
2026-10-16T23:57:50.582Z,38.2992,20.6656,10,4.3,mwr,17,66,0.75,0.95,us,us6000u2ww,2026-11-03T05:57:08.665Z,"5 km NNE of Sámi, Greece",earthquake,,,,,,reviewed,us,us
```

## 3. Batas pengiriman

| Parameter | Nilai | Alasan |
|---|---|---|
| Ukuran kelompok | 100 event | Satu putaran baca dan kirim, memudahkan pencocokan dengan log |
| Partition key | `id` | Satu kejadian selalu mendarat di partisi yang sama |
| Laju maksimum | 200 event per detik | Menjaga namespace Standard 1 throughput unit tetap di bawah batas |
| Percobaan ulang | 3 kali | Selang 1, 2, dan 4 detik, hanya untuk galat yang dapat dicoba ulang |

Satu kelompok 100 event dikirim sebagai 100 pesan dengan partition key masing-masing, bukan satu batch AMQP. Batch AMQP hanya boleh memuat satu partition key, sehingga 100 `id` berbeda tidak dapat disatukan dalam satu batch.

## 4. Catatan kualitas data yang perlu diketahui konsumen

1. **Jenis kejadian tidak seragam.** Dari 298.930 baris Bronze, hanya 296.308 bertipe `earthquake`. Sisanya `mining explosion` (2.204), `ice quake` (126), `quarry blast` (27), `explosion` (51), `volcanic eruption` (74), dan beberapa tipe lain. Penyaringan `type = 'earthquake'` dilakukan di Silver, bukan di produser, agar data mentah tetap utuh.
2. **Satu baris tanpa `magType`.** Di seluruh Bronze hanya ada satu baris dengan `magType` kosong. Nilainya dikirim sebagai `null`.
3. **`id` dapat berulang di batas jendela.** Parameter `endtime` USGS bersifat inklusif sampai tengah malam, sehingga 142 baris muncul di dua jendela bersebelahan. Deduplikasi memakai `id` dan `updated` terbaru dilakukan di Silver.
4. **Nama tempat memuat aksara non-Latin.** Field `place` dikirim apa adanya, tanpa transliterasi.

## 5. Cara memakai

Produser: `producer/replay_bronze.py`. Penjelasan menjalankan, batas laju, arti metric, dan penanganan galat ada di `docs/streaming.md`.
