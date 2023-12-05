# SpaceX Satellite Tracker

This application allows you to track SpaceX satellites using data from the Timescale DB.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Docker
- Python 3.8 or higher
- A virtual environment tool like `venv` or `virtualenv`

### Installation

1. Start the Timescale DB Docker container by running the following command in your terminal:

```bash
docker compose -f timescaledb/docker-compose.yaml up
```

2. Create a virtual environment and activate it:

```bash
python3 -m venv env
source env/bin/activate  # On Windows, use `env\Scripts\activate`
```

3. Install the required Python packages:
```bash
pip install -r requirements.txt
```

3. Create an .env file in the root folder with the following info:
```
POSTGRES_USER = timescaledb
POSTGRES_PASSWORD = password
POSTGRES_HOST = localhost
POSTGRES_PORT = 5432
```

### Usage
1. Edit the queries in the main.py file. The docstrings in this file explain how each query works. You can use these as a guideline to create your own query functions using the CRUD operations in the operations.py file.

2. Run the main script:

```bash
python main.py
```

You can also run any other specific file that you might want.

## Next steps

- Build an actual API for the requests instead of just relying on the Python structure.
- Write tests for the CRUD operations.
- Do some tests to understand how much of a gain is to actually be using TimescaleDB's hypertables instead of a regular PostgreSQL table.
