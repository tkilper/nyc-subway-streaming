---
name: NYC Subway Streaming Pipeline State
description: Full architecture, Flink jobs, Postgres schema, Makefile commands, dashboard, and next steps
type: project
---

## Architecture

- **Broker**: Redpanda (Kafka-compatible) — `redpanda-1:29092` internal, `localhost:9092` external
- **Stream processor**: Apache Flink 1.20.2 (PyFlink), jobmanager UI at `localhost:8081`
- **Database**: Postgres 14 at `localhost:5432` — db/user/password all `postgres` (dev defaults). Flink jobs read `POSTGRES_URL`/`POSTGRES_USER`/`POSTGRES_PASSWORD` env (defaults `jdbc:postgresql://postgres:5432/postgres` + `postgres`/`postgres`); dashboard reads `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_DB`/`POSTGRES_USER`/`POSTGRES_PASSWORD`. No real secrets in the repo; `.claude/settings.local.json` and `*:Zone.Identifier` are gitignored.
- **Producer**: `src/producers/send_mta_data.py` — runs on host (`make producer`), polls all 8 MTA GTFS-RT subway feeds (`FEED_URLS`: numbered lines, ACE, BDFM, G, JZ, NQRW, L, SIR) every 30s, publishes to `updates-data` and `vehicle-data` topics. A feed that fails twice in a row is skipped for that poll; the others still publish.

## Flink Jobs (all in `src/job/`)

| Job file | Kafka topic | Sink table | Notes |
|---|---|---|---|
| `insert_job_update.py` | `updates-data` | `processed_stop_updates` | Parses trip_update entities |
| `insert_job_vehicle.py` | `vehicle-data` | `processed_vehicle` | Parses vehicle position entities |
| `trip_tracking_job.py` | both | `trip_tracking` | Interval join of vehicle + stop updates |
| `anomaly_job.py` | `updates-data` | `trip_delay_anomalies` | ARIMA(1,1,1) anomaly detection keyed by route_id |

## Anomaly Job (upgraded to ARIMA)

- Replaced 5-minute tumbling window + static threshold with `KeyedProcessFunction` keyed by `route_id`
- Maintains rolling 200-observation delay history per route in Flink ListState
- Fits ARIMA(1,1,1) after 50 observations; flags anomaly when `|actual - predicted| > 3σ`
- Emits one row per stop event; `predicted_delay`/`residual` are NULL until warm-up complete
- `statsmodels` and `numpy` added to `requirements.txt`

## Postgres Tables (`create_tables.sql`)

- `processed_vehicle` — vehicle positions
- `processed_stop_updates` — stop-level arrival/departure times and delays
- `trip_tracking` — latest trip state (interval join result); `current_status`: 0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO
- `trip_delay_anomalies` — ARIMA anomaly scores: `trip_id, route_id, start_date, stop_id, stop_sequence, arrival_delay, predicted_delay, residual, is_anomaly`

## Makefile Commands

| Command | What it does |
|---|---|
| `make up` | Build and start all Docker services |
| `make down` | Stop and remove containers |
| `make producer` | Run the host-side MTA producer (`python -m src.producers.send_mta_data`) |
| `make tables` | Run `create_tables.sql` against Postgres |
| `make all_jobs` | Submit all 4 Flink jobs (single docker exec session) |
| `make cancel_jobs` | Cancel all running Flink jobs |
| `make reset_volumes` | `docker compose down --volumes` — wipes Postgres + Redpanda data |
| `make dashboard` | Install dashboard deps and launch Streamlit |

## Dashboard (`dashboard/app.py`)

- Streamlit app, runs at `localhost:8501` via `make dashboard`
- Two tabs: **Live Trips** and **Anomaly Detection**
- Both use `@st.fragment(run_every=30)` for independent 30s auto-refresh
- Title: "NYC MTA Subway Trip Tracking and Alerts - All Lines"
- Live Trips: route multiselect filter (empty = all routes); active trip count, route count metrics; active trips by route table; full trip table with status, delay, time to next stop
- Anomaly Detection: events/scored/anomaly metrics; flagged anomalies table; all scored events expander; anomalies by route bar chart
- Connects directly to Postgres on `localhost:5432`

## Known Issues / Notes

- `all_jobs` uses a single `docker compose exec` bash session to avoid Make stopping on non-zero exit; each `flink run -py` blocks until submitted so the session approach is required
- `classloader.check-leaked-classloader: false` set in both jobmanager and taskmanager FLINK_PROPERTIES to suppress benign shutdown log error
- ARIMA is refit on every event per route — may cause CPU spikes on taskmanager; consider refitting every N events if that becomes an issue
- `Time to Stop` column shows `—` for trips with no matching row in `processed_stop_updates`
