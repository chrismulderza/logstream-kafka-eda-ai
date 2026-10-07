# Multi-stage UBI9 image for the predictive AI stream worker.
# Build (example):
#   podman build -t IMAGE_REF -f worker/Dockerfile worker
FROM registry.access.redhat.com/ubi9/python-311:1 AS builder

USER 0
WORKDIR /build
COPY requirements.txt .
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

FROM registry.access.redhat.com/ubi9/python-311:1

USER 0
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp

COPY --from=builder /opt/venv /opt/venv
WORKDIR /opt/app-root/src
COPY stream_worker ./stream_worker

# OpenShift restricted-v2 assigns an arbitrary UID in the root group.
RUN chgrp -R 0 /opt/app-root /opt/venv \
    && chmod -R g=u /opt/app-root /opt/venv

USER 1001
EXPOSE 8080
CMD ["python", "-m", "stream_worker"]
