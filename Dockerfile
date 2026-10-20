FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen
COPY superspider/ ./superspider/
COPY crawlers/ ./crawlers/
COPY main.py .
CMD ["uv", "run", "python", "main.py"]
