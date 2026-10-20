FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen
COPY superspider/ ./superspider/
COPY crawlers/ ./crawlers/
COPY main.py .
# 运行时直接用 venv 解释器：python 成为 PID1，SIGTERM 优雅停止不被中间层截断
# （uv run 作为 PID1 收到 TERM 会先于子进程退出，容器随之被杀，清理跑不完）
CMD ["/app/.venv/bin/python", "main.py"]
