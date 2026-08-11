FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.lock pyproject.toml README.md /tmp/wikitrend/
COPY src /tmp/wikitrend/src
RUN pip install --no-cache-dir -r /tmp/wikitrend/requirements.lock \
    && pip install --no-cache-dir --no-deps -e /tmp/wikitrend
