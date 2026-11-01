FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 OMP_THREAD_LIMIT=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 PYTHONPATH=/app/src
RUN apt-get -o Acquire::Retries=1 -o Acquire::http::Timeout=20 update && apt-get -o Acquire::Retries=1 -o Acquire::http::Timeout=20 install -y --no-install-recommends tesseract-ocr tesseract-ocr-eng tini && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir Pillow==13.0.0
COPY src/stream_v3/ /app/src/stream_v3/
USER 1000:1000
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "python3", "-m", "stream_v3.twitch_thumbnail_worker"]
CMD ["--config", "/config/config.json", "--state-root", "/state", "--listen", "0.0.0.0:9125"]
