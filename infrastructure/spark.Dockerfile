FROM bitnami/spark:4.0.1

USER root
WORKDIR /opt/wikitrend

COPY requirements.lock pyproject.toml README.md ./
COPY src ./src
RUN pip3 install --no-cache-dir -r requirements.lock \
    && pip3 install --no-cache-dir --no-deps -e .

USER 1001
