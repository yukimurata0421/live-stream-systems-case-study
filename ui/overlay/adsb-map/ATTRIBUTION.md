# ADS-B map data and software attribution

The on-screen footer is intentionally always visible because this map is the
focus of a public video stream.

- Map rendering: MapLibre GL JS 6.1.0, BSD-3-Clause. The bundled licence is in
  `vendor/LICENSE-maplibre-gl.txt`.
- Vector tiles: OpenFreeMap / OpenMapTiles, using OpenStreetMap data under the
  Open Database Licence. Required credit:
  `OpenFreeMap © OpenMapTiles · Data © OpenStreetMap contributors (ODbL)`.
  See <https://openfreemap.org/> and
  <https://www.openstreetmap.org/copyright>.
- Terrain tiles: Mapterhorn. Japan coverage includes GSI Japan elevation data
  under the GSI content terms and Survey Act approval R 7JHs 542. See
  <https://mapterhorn.com/attribution/> and
  <https://download.mapterhorn.com/attribution.json>.
- Airport points: OurAirports `airports.csv`, released into the Public Domain.
  Only scheduled-service large airports inside the fixed stream viewport are
  included in `airports.geojson`.
- Precipitation: Japan Meteorological Agency, High-resolution Precipitation
  Nowcast analysis image. The layer uses only the analysis timestamp where
  `basetime == validtime`; no forecast frames are displayed. Source colors are
  recolored and assigned intensity-dependent transparency, so the permanent
  on-screen credit states `Precipitation: JMA, processed (PDL1.0)`. See
  <https://www.jma.go.jp/bosai/nowc/>,
  <https://www.jma.go.jp/jma/kishou/info/coment.html>, and
  <https://www.digital.go.jp/resources/open_data/public_data_license_v1.0>.

The local overlay server proxies and bounds the vector and terrain tile
requests. OpenFreeMap currently permits public and commercial use without an
API key, but does not provide an SLA. The proxy boundary allows a future
self-hosted OpenFreeMap endpoint to replace the public service without changing
the browser map.

The JMA website tile route is an operational website interface rather than a
contracted API and may change without notice. `JMA_NOWC_METADATA_URL` and
`JMA_NOWC_DATA_ROOT_URL` are therefore configurable. For a commercial service
that requires an availability contract, replace this route with formally
distributed GRIB2 or a licensed provider while keeping the same local status
and tile boundary.
