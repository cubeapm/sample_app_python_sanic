# Python Sanic OpenTelemetry Instrumentation

This is a sample app to demonstrate how to instrument Python Sanic app with OpenTelemetry. It contains source code for the Sanic app which interacts with various services like Redis, MySQL, Kafka, etc. to demonstrate tracing for these services. This repository has a docker compose file to set up all these services conveniently.

This repository is inentionally designed to work with any OpenTelemetry backend, not just CubeAPM. In fact, it can even work without any OpenTelemetry backend (by dumping traces to console, which is also the default behaviour).

## Setup

Clone this repository and go to the project directory. Then run the following commands

```
python3 -m venv .
source ./bin/activate
pip install -r requirements.txt
docker compose up --build

# Run the following command in a separate terminal to apply the database migrations.
# This is only needed during first time setup. Repeat executions are harmless though.
alembic upgrade head
```

## Contributing

Please feel free to raise PR for any enhancements - additional service integrations, library version updates, documentation updates, etc.
