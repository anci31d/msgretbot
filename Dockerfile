FROM python:3.14-slim AS builder
WORKDIR /build
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir --prefix=/install -r requirements.lock
COPY pyproject.toml ./
COPY msgretbot ./msgretbot
RUN python -m pip install --no-cache-dir --no-deps --prefix=/install .

FROM python:3.14-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN groupadd --gid 1000 app && useradd --uid 1000 --gid app --no-create-home app \
    && mkdir /app/data /app/logs && chown app:app /app/data /app/logs
COPY --from=builder /install /usr/local
USER 1000:1000
STOPSIGNAL SIGTERM
ENTRYPOINT ["python", "-m", "msgretbot"]
CMD ["run", "--config", "config.toml"]
