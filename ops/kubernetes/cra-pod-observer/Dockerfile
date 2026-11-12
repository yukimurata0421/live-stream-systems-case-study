FROM python:3.13-slim@sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pod_api_observer.py /app/pod_api_observer.py
COPY pod_api_health.py /app/pod_api_health.py
# Precompile imported modules once. Read-only Pods must not compile them on
# every bounded API child or exec probe under a small CPU quota.
RUN env -u PYTHONDONTWRITEBYTECODE python3 -c "import pod_api_observer, pod_api_health"
USER 992:983
ENTRYPOINT ["python3", "/app/pod_api_observer.py"]
CMD ["serve"]
