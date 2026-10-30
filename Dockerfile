# PharmaSafe: pipeline runner, FastAPI service and Streamlit dashboard in one image.
# Data (data/, reports/) is NOT baked in; mount it as a volume (see docker-compose.yml).
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN ln -sfn "$(dirname $(dirname $(readlink -f $(which java))))" /opt/java && true
ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY configs ./configs
COPY src ./src
COPY scripts ./scripts
COPY api ./api
COPY dashboard ./dashboard
COPY docs ./docs

EXPOSE 8501 8000
# Default: dashboard. Override for the API or pipeline:
#   docker run ... pharmasafe uvicorn api.main:app --host 0.0.0.0 --port 8000
#   docker run ... pharmasafe python scripts/run_full_pipeline.py
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
