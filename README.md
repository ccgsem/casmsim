# casmsim

[![Release](https://img.shields.io/github/v/release/clinejc/casmsim)](https://img.shields.io/github/v/release/clinejc/casmsim)
[![Build status](https://img.shields.io/github/actions/workflow/status/clinejc/casmsim/main.yml?branch=main)](https://github.com/clinejc/casmsim/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/clinejc/casmsim/branch/main/graph/badge.svg)](https://codecov.io/gh/clinejc/casmsim)
[![Commit activity](https://img.shields.io/github/commit-activity/m/clinejc/casmsim)](https://img.shields.io/github/commit-activity/m/clinejc/casmsim)
[![License](https://img.shields.io/github/license/clinejc/casmsim)](https://img.shields.io/github/license/clinejc/casmsim)

`casmsim` is a Python framework for implementing agent-based models that simulate the dynamics of a synthetic population

- **Github repository**: <https://github.com/clinejc/casmsim/>
- **Documentation** <https://clinejc.github.io/casmsim/>

## Installation

Install the environment with

```bash
export CC=mpicxx; export CXX=mpicxx
make install
```

To build a Docker image for `casmsim`:

* on the MITRE network

    ```bash
    docker build -t casmsim . -f Dockerfile-mitre
    ```

* off the MITRE network

    ```bash
    docker build -t casmsim . -f Dockerfile
    ```

## Launch the modeling environment:
First create the virtual environments with

```bash
% python -m venv .venv
```

To launch the virtualenv, run

```bash
% source ./.venv/bin/activate
(casmsim) ...
```

## Quickstart: running the model
There are three ways to run the model

1. Run from the command line using `uv run`
2. Run fromm the command line using virtualenv
3. Run from

To run (option 1):

```bash
% uv run mpirun -n 1 python -m casmsim.runner config/casmsim.yaml
```

To run with the virtual environment (option 2):

```bash
% source ./.venv/bin/activate
(casmsim)
(casmsim) mpirun -n 1 python -m casmsim.runner config/casmsim.yaml
....
(casmsim) deactivate
%
```

---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
