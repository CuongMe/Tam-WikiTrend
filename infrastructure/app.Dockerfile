FROM python:3.11-slim

WORKDIR /app
COPY requirements.lock pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -r requirements.lock \
    && pip install --no-cache-dir --no-deps -e .

COPY api ./api
COPY dashboard ./dashboard

EXPOSE 8000 8501
