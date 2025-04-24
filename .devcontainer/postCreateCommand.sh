#! /usr/bin/env bash

# Initialize git if not already initialized
if [ ! -d .git ]; then
    git init
    git config --global user.email "dev@example.com"
    git config --global user.name "Dev Container"
fi

# Install Dependencies
uv sync

# Install pre-commit hooks
uv run pre-commit install --install-hooks
