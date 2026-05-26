FROM python:3.12-slim

RUN pip install uv 

# Add code location code
WORKDIR /opt/dagster/app
# 1. Install dependencies only (no source needed) — this layer is cached unless uv.lock changes.
COPY batch_elt_components/pyproject.toml batch_elt_components/uv.lock ./
RUN uv sync --no-install-project
# 2. Copy the source, then install the project itself (fast — source is now present).
COPY batch_elt_components /opt/dagster/app
RUN uv sync
COPY dbt /opt/dagster/dbt
COPY duckdb /opt/dagster/duckdb

# Run dagster code server on port 4000
EXPOSE 4000

HEALTHCHECK --timeout=1s --start-period=3s --interval=3s --retries=20 CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]

# CMD allows this to be overridden from run launchers or executors to execute runs and steps
CMD ["uv","run","dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "batch_elt_components.definitions"]