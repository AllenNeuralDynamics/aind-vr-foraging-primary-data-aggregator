# hash:sha256:26ef0e7012e726e03971aff3896036a14cf27db885ddd6599ca346249fecb4f1
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/ubuntu:26.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-ask-pass /

RUN apt-get update \
    && apt-get install --no-install-recommends -y curl ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"
COPY ./pyproject.toml ./uv.lock ./.python-version ./
RUN uv sync --locked
