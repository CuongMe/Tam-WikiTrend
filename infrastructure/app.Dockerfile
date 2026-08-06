FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e .

COPY api ./api
COPY dashboard ./dashboard

EXPOSE 8000 8501

