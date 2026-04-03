.PHONY: build up down job aggregation_job stop start

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

stop:
	docker compose stop

start:
	docker compose start