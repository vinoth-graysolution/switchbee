# ─────────────────────────────────────────────────────────────
# Stage 1 – Builder
#   Install uv via pip, sync dependencies, pre-download NLTK data
# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Install uv via pip (no external registry needed)
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock ./

# Create a virtual environment and install all dependencies
# --frozen  → respect uv.lock exactly
# --no-dev  → skip dev-only dependencies
RUN uv sync --frozen --no-dev

# Pre-download NLTK punkt_tab tokenizer into /app/nltk_data
# (avoids permission errors at runtime when running as non-root)
RUN /app/.venv/bin/python -c "import nltk; nltk.download('punkt_tab', download_dir='/app/nltk_data')"

# ─────────────────────────────────────────────────────────────
# Stage 2 – Runtime
#   Lean final image; only the venv + NLTK data are copied over
# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Non-root user for security
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

# Copy the pre-built virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy pre-downloaded NLTK data
COPY --from=builder /app/nltk_data /app/nltk_data

# Copy application source code (owned by appuser so it can write CSVs at runtime)
COPY --chown=appuser:appgroup src/ ./src/
COPY --chown=appuser:appgroup main.py ./

# Make the venv's executables take priority on PATH
# Point NLTK_DATA to the pre-downloaded directory
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    NLTK_DATA=/app/nltk_data

# Switch to non-root user
USER appuser

# Expose the uvicorn port
EXPOSE 7860

# Run the FastAPI app via uvicorn
CMD ["uvicorn", "src.service:app", "--host", "0.0.0.0", "--port", "7860"]
