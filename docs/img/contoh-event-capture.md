# Contoh event hasil Capture

Lima event pertama yang terbaca dari file Avro Capture di `bronze/usgs_stream`. Field sudah sesuai kontrak di `docs/event-schema.md`.

```json
{"id": "us100062gd", "time": "2016-07-10T21:33:55.890Z", "updated": "2022-08-03T03:38:09.216Z", "latitude": 30.8996, "longitude": 66.4706, "depth_km": 30.09, "mag": 4.7, "mag_type": "mb", "place": "2 km SE of Chaman, Pakistan", "type": "earthquake"}
{"id": "us100062fl", "time": "2016-07-10T20:00:38.190Z", "updated": "2016-09-08T01:50:26.040Z", "latitude": -21.0831, "longitude": -68.9674, "depth_km": 114.44, "mag": 4.5, "mag_type": "mwr", "place": "152 km N of Calama, Chile", "type": "earthquake"}
{"id": "us100062ew", "time": "2016-07-10T17:01:13.680Z", "updated": "2016-09-08T01:50:26.040Z", "latitude": -4.5361, "longitude": 153.2201, "depth_km": 59.14, "mag": 4.7, "mag_type": "mb", "place": "107 km ESE of Kokopo, Papua New Guinea", "type": "earthquake"}
{"id": "us20006ez6", "time": "2016-07-10T15:59:49.820Z", "updated": "2016-09-08T01:50:26.040Z", "latitude": -61.3274, "longitude": -38.1392, "depth_km": 10.0, "mag": 4.4, "mag_type": "mb", "place": "Scotia Sea", "type": "earthquake"}
```

Sumber: `bronze/usgs_stream/earthquake-space/usgs-events/0/2026/11/24/14/16/24.avro`. Dibaca dengan `fastavro`, kolom `Body` diurai sebagai JSON.
