# Ringkasan bukti streaming

Dihasilkan dari file Avro Capture di `bronze/usgs_stream`, dibaca langsung dari storage.

## Event unik per partisi

| Partisi | Jumlah event unik |
|---|---:|
| 0 | 4967 |
| 1 | 5064 |
| 2 | 4918 |
| 3 | 5052 |

Total sebelum deduplikasi: 20001. Setelah deduplikasi berdasarkan `id`: 10000.

## Field event

Semua event memakai sepuluh field berikut, sesuai `docs/event-schema.md`.

```text
depth_km id latitude longitude mag mag_type place time type updated
```

## Sumber

- 28 berkas Avro pada 2026-11-24, pukul 14:16 sampai 14:50 UTC.
- Dibaca dengan `fastavro`, kolom `Body` diurai sebagai JSON.
- Log pengiriman: `docs/replay_10k.log`, 10.000 terkirim, 0 gagal.
