# SocialModel: Agent-Based Simulation with Repast4Py

## Overview
`SocialModel` is a parallel agent-based simulation built using [Repast4Py](https://repast.github.io/repast4py.html). It models interactions between `Person` and `Place` agents across a spatial grid using hourly environmental inputs and configurable simulation parameters.

## Model Entities

### Person
- Attributes:
  - `energy`: changes based on daily and weekly routines
  - `environment`: a vector (e.g., temperature)
  - `social_links`: list of person IDs
  - `place_id`: current associated place
- Behavior:
  - Moves between places each tick
  - Reacts to hourly and weekday context

### Place
- Attributes:
  - `x`, `y`: UTM spatial coordinates
  - `latitude`, `longitude`: geographic location
  - `is_remote`: outside the bounding grid

## Spatial Projections
- `SharedCSpace`: continuous agent positioning
- `SharedGrid`: discrete grid cells
- Both use `BorderType.Sticky` and are initialized from environment bounds

## Environment Input
- Reads `temperature` data from a NetCDF file `environment.nc`
- Updates every simulation tick using hourly time steps
- Temperature values are assigned to person agents based on location

## Scheduling
- Schedule runner initialized with MPI rank:
  ```python
  self.runner = schedule.init_schedule_runner(comm)
  self.runner.schedule_repeating_event(1, 1, self.step)
  ```
- Tick execution is managed using `runner.execute()`

## Parameterization
Parameters are loaded via YAML with Repast4Py’s built-in parser:

```yaml
num_persons: 1000
num_places: 100
grid_resolution: 1000
```

- Required keys: `num_persons`, `num_places`, `grid_resolution`
- Validated in the model constructor with defaults and errors for missing values

### CLI Example
Run the model using:
```bash
mpirun -n 4 python social_model.py -pf parameters.yml
```

## Optimizations
- Indexed lookup dictionaries:
  - `self.place_index`: maps place IDs to agents
  - `self.person_index`: maps person IDs to agents
- Efficient group-wise updates by place using precomputed offsets

## Temporal Logic
- Simulated time starts at 2025-01-01
- Time step: one hour
- Day-of-week and hour used to define behavior logic

---
For more details, see the source file `social_model.py`.

