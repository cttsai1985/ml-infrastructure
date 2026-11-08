FROM python:3.12.16-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never \
    PATH="/opt/venv/bin:$PATH" \
    OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
RUN python -m pip install --no-cache-dir uv==0.10.4
WORKDIR /opt/platform
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project --no-cache
COPY src ./src
COPY configs ./configs
COPY docker ./docker
RUN uv sync --frozen --no-dev --no-cache
ENTRYPOINT ["python", "docker/entrypoint.py"]
