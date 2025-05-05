# === VARIABLES ===
APP_NAME=spx-fetcher
DOCKER_PORT=8080
LOCAL_PORT=8000
ENV_FILE=.env
SERVICE_KEY=/etc/scecrets/gcp-service-account.json

# === INSTALL & CLEAN ===
install:
	pip install -r requirements.txt

venv:
	python -m venv venv && source venv/bin/activate && pip install -r requirements.txt

clean:
	find . -type d -name '__pycache__' -exec rm -r {} +
	rm -rf .pytest_cache logs/*.log

# === RUN LOCALLY ===
run:
	PYTHONPATH=. uvicorn app.main:app --reload

logs:
	tail -f logs/fetcher.log

test:
	pytest tests/

lint:
	black app/ dashboard/ tests/ analytics/ workers/ common/
	isort app/ dashboard/ tests/ analytics/ workers/ common/

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
	@echo "  make install        Install dependencies"
	@echo "  make venv           Create virtualenv & install"
	@echo "  make run            Run FastAPI server locally"
	@echo "  make docker-build   Build Docker image"
	@echo "  make docker-run     Run Docker container with env and key"
	@echo "  make logs           Tail log output"
	@echo "  make test           Run tests"
	@echo "  make lint           Format code with black + isort"
	@echo "  make clean          Remove cache files"
