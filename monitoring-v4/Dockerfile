FROM python@sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/opt/stream-v4/src

WORKDIR /opt/stream-v4
COPY requirements/runtime-py313-linux-amd64.lock ./requirements/runtime.lock
COPY src ./src
RUN test "$(uname -m)" = "x86_64" \
    && python -c 'import sys; assert sys.version_info[:2] == (3, 13)' \
    && python -m pip install --no-cache-dir --no-deps --require-hashes \
        -r requirements/runtime.lock \
    && chmod -R a=rX /opt/stream-v4/src \
    && groupadd --gid 1000 streamv4 \
    && useradd --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin streamv4

USER 1000:1000
ENTRYPOINT ["python", "-m"]
