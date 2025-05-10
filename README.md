# Datadog Instrumentation

This branch contains code for Datadog instrumentation.

By default, hitting an API endpoint will generate a trace, which is sent to CubeAPM. This behavior is controlled via environment variables in [docker-compose.yml](docker-compose.yml).

Refer the project README below for more details.

## Troubleshooting

If the app does not show up in CubeAPM after integration is done, add the below environment variables to check Datadog tracer logs.

```shell
# Print Datadog tracer startup logs on screen
DD_TRACE_STARTUP_LOGS=true

# Enable Datadog tracer debug logging if needed to see detailed logs
#DD_TRACE_DEBUG=true
```

---

# Python Sanic Instrumentation

This is a sample app to demonstrate how to instrument Python Sanic app with **New Relic** and **OpenTelemetry**. It contains source code for the Sanic app which interacts with various services like Redis, MySQL, Kafka, etc. to demonstrate tracing for these services. This repository has a docker compose file to set up all these services conveniently.

The code is organized into multiple branches. The main branch has the Sanic app without any instrumentation. Other branches then build upon the main branch to add specific instrumentations as below:

| Branch                                                                                         | Instrumentation | Code changes for instrumentation                                                                                |
| ---------------------------------------------------------------------------------------------- | --------------- | --------------------------------------------------------------------------------------------------------------- |
| [main](https://github.com/cubeapm/sample_app_python_sanic/tree/main)         | None            | -                                                                                                               |
| [datadog](https://github.com/cubeapm/sample_app_python_sanic/tree/datadog) | Datadog       | [main...datadog](https://github.com/cubeapm/sample_app_python_sanic/compare/main...datadog) |
| [elastic](https://github.com/cubeapm/sample_app_python_sanic/tree/elastic)         | Elastic   | [main...elastic](https://github.com/cubeapm/sample_app_python_sanic/compare/main...elastic)         |
| [newrelic](https://github.com/cubeapm/sample_app_python_sanic/tree/newrelic) | New Relic       | [main...newrelic](https://github.com/cubeapm/sample_app_python_sanic/compare/main...newrelic) |
| [otel](https://github.com/cubeapm/sample_app_python_sanic/tree/otel)         | OpenTelemetry   | [main...otel](https://github.com/cubeapm/sample_app_python_sanic/compare/main...otel)         |

# Setup

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

Sanic app will now be available at `http://localhost:8000`.

The app has various API endpoints to demonstrate integrations with Redis, MySQL, Kafka, etc. Check out [server.py](server.py) for the list of API endpoints.

# Contributing

Please feel free to raise PR for any enhancements - additional service integrations, library version updates, documentation updates, etc.
