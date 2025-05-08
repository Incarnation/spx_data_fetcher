# === VARIABLES ===
APP_NAME=spx-fetcher
DOCKER_PORT=8050
LOCAL_PORT=8050
ENV_FILE=.env
SERVICE_KEY=etc/secrets/gcp-service-account.json

# === INSTALL & CLEAN ===
install:
	pip install -r requirements.txt

venv:
	python -m venv venv && source venv/bin/activate && pip install -r requirements.txt

clean:
	find . -type d -name '__pycache__' -exec rm -r {} +
	rm -rf .pytest_cache logs/*.log

# === RUN LOCALLY ===
run-worker:
	PYTHONPATH=. python3 workers/main.py

run-dashboard:
	PYTHONPATH=. python3 dashboard/main.py

logs:
	tail -f logs/fetcher.log

test:
	pytest tests/

lint:
	black fetcher/ dashboard/ tests/ analytics/ workers/ common/ trade/
	isort fetcher/ dashboard/ tests/ analytics/ workers/ common/ trade/

# === DOCKER ===
docker-build:
	docker build -t $(APP_NAME) .

docker-run:
	docker run \
		--env-file $(ENV_FILE) \
		-v $(PWD)/$(SERVICE_KEY):/gcp.json \
		-e GOOGLE_APPLICATION_CREDENTIALS=/gcp.json \
		-p $(LOCAL_PORT):$(DOCKER_PORT) \
		$(APP_NAME)

# === HELPER ===
help:
	@echo "Usage:"
	@echo "  make install           Install dependencies"
	@echo "  make venv              Create virtualenv & install"
	@echo "  make run-worker        Run background worker locally"
	@echo "  make run-dashboard     Run Dash dashboard locally"
	@echo "  make docker-build      Build Docker image"
	@echo "  make docker-run        Run Docker container with env and key"
	@echo "  make logs              Tail log output"
	@echo "  make test              Run tests"
	@echo "  make lint              Format code with black + isort"
	@echo "  make clean             Remove cache files"
