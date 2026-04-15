PROJECT_NAME=retail-lakehouse

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d

logs:
	docker compose logs -f

logs-airflow:
	docker compose logs -f airflow-webserver airflow-scheduler

logs-scheduler:
	docker compose logs -f airflow-scheduler

logs-webserver:
	docker compose logs -f airflow-webserver

logs-spark:
	docker compose logs -f spark

ps:
	docker compose ps

init:
	docker compose up airflow-init

bootstrap:
	chmod +x scripts/bootstrap_local.sh
	./scripts/bootstrap_local.sh

smoke:
	chmod +x scripts/smoke_test.sh
	./scripts/smoke_test.sh

clean:
	docker compose down -v --remove-orphans

reset:
	docker compose down -v --remove-orphans
	docker compose up airflow-init
	docker compose up -d