# Install uv
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install necessary system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    mpich \
    libmpich-dev \
    libomp-dev

# Change the working directory to the `app` directory
WORKDIR /app

# Copy the lockfile and `pyproject.toml` into the image
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy the project into the image
COPY . /app

# Sync the project
RUN uv sync --frozen

CMD ["mpirun", "-n 1", "python", "-m casmsim.social_model", "config/casmsim.yaml" ]
