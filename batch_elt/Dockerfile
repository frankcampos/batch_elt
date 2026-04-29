FROM python:3.12-slim

RUN pip install uv 


# Add code location code
WORKDIR /opt/dagster/app
COPY batch_elt /opt/dagster/app
COPY dbt /opt/dagster/dbt
COPY duckdb /opt/dagster/duckdb
RUN uv sync 
# Run dagster code server on port 4000
EXPOSE 4000

HEALTHCHECK --timeout=1s --start-period=3s --interval=3s --retries=20 CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]

# CMD allows this to be overridden from run launchers or executors to execute runs and steps
CMD ["uv","run","dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "batch_elt.definitions"]