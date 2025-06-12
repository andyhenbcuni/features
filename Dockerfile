FROM python:3.12-slim as base
WORKDIR /app
RUN pip install --no-cache-dir --upgrade uv

FROM base as builder
COPY pyproject.toml .
RUN uv sync --group common --group managed-table-service --group actions-sdk

FROM base as release
ENV PYTHONPATH=$PYTHONPATH:src/
COPY --from=builder /app/.venv ./.venv/
COPY  /src ./src
