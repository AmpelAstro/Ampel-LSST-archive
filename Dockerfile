
# renovate: datasource=python-version depName=python versioning=python
ARG PYTHON_VERSION=3.14.3

FROM python:$PYTHON_VERSION-slim AS base

WORKDIR /app

FROM base AS builder

# renovate: datasource=pypi depName=poetry versioning=pep440
ARG POETRY_VERSION=2.3.2

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install "poetry==$POETRY_VERSION"
RUN python -m venv /venv

RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock ./
RUN VIRTUAL_ENV=/venv poetry install --no-root --no-directory --all-extras --without dev

# pre-install duckdb extensions
RUN echo "import duckdb\nfor ext in ['httpfs', 'iceberg', 'avro']: duckdb.install_extension(ext); duckdb.load_extension(ext)" | /venv/bin/python

COPY ampel ampel
COPY README.md README.md
RUN poetry build && /venv/bin/pip install dist/*.whl

FROM base AS final

# create cache dirs for astropy and friends
RUN mkdir -p --mode a=rwx /var/cache/astropy
ENV XDG_CACHE_HOME=/var/cache XDG_CONFIG_HOME=/var/cache MPLBACKEND=Agg

RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.duckdb /root/.duckdb
COPY --from=builder /venv /venv
CMD ["/venv/bin/uvicorn", "ampel.lsst.archive.server.app:app", "--host", "0.0.0.0", "--port", "80"]

EXPOSE 80
