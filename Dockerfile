# syntax=docker/dockerfile:1

FROM python:3.11-slim AS builder

# uv installs from the committed lockfile so the image gets the same versions
# the test suite ran against. --frozen, not --locked: install exactly what
# uv.lock pins without re-validating it, because the committed lockfile carries
# an exclude-newer span that has since elapsed.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies change far less often than source, so install them in their own
# layer and keep rebuilds after a code edit cheap.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev


FROM python:3.11-slim AS runtime

# Nothing in the image needs to write outside the venv now that state lives
# in PostgreSQL, so run as an unprivileged user.
RUN useradd --create-home --uid 10001 relay

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY --from=builder --chown=relay:relay /app/.venv /app/.venv
COPY --chown=relay:relay *.py dashboard.html ./

USER relay

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=2).status == 200 else 1)"

# 0.0.0.0 is required: uvicorn's 127.0.0.1 default binds the container's own
# loopback, which -p cannot reach from the host.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
