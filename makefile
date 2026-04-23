.PHONY: build up down job aggregation_job stop start tables all_jobs cancel_jobs reset_volumes

build:
	docker compose build

up:
	docker compose up --build --remove-orphans -d

down:
	docker compose down --remove-orphans

insert_update_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/insert_job_update.py --pyFiles /opt/src -d

insert_vehicle_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/insert_job_vehicle.py --pyFiles /opt/src -d

trip_tracking_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/trip_tracking_job.py --pyFiles /opt/src -d

anomaly_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/anomaly_job.py --pyFiles /opt/src -d

all_jobs:
	docker compose exec --detach jobmanager ./bin/flink run -py /opt/src/job/insert_job_update.py --pyFiles /opt/src
	docker compose exec --detach jobmanager ./bin/flink run -py /opt/src/job/insert_job_vehicle.py --pyFiles /opt/src
	docker compose exec --detach jobmanager ./bin/flink run -py /opt/src/job/trip_tracking_job.py --pyFiles /opt/src
	docker compose exec --detach jobmanager ./bin/flink run -py /opt/src/job/anomaly_job.py --pyFiles /opt/src

cancel_jobs:
	docker compose exec jobmanager bash -c "./bin/flink list -r 2>/dev/null | grep -oE '[0-9a-f]{32}' | xargs -r -I{} ./bin/flink cancel {}"

tables:
	docker compose exec -T postgres psql -U postgres -d postgres -f - < create_tables.sql

reset_volumes:
	docker compose down --volumes --remove-orphans

stop:
	docker compose stop

start:
	docker compose start