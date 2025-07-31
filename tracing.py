# ref: https://github.com/krvajal/sanic-opentelemetry-python-example
# ref: https://dev.to/sjsadowski/honeycomb-python-and-i-an-opentelemetry-horror-story-with-a-happy-ending-3hmc

import os
from opentelemetry import trace, context
from opentelemetry.sdk import resources
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from sanic import HTTPResponse, Request, Sanic
from socket import gethostname

def instrument_app(app: Sanic):
    resource = resources.Resource.create({
        resources.HOST_NAME: gethostname() or 'UNSET',
    })

    if os.getenv('OTEL_LOG_LEVEL', '') == 'debug':
        trace_processor = SimpleSpanProcessor(ConsoleSpanExporter())
    else:
        trace_processor = BatchSpanProcessor(OTLPSpanExporter())
    trace_provider = TracerProvider(
        resource=resource,
        active_span_processor=trace_processor
    )
    trace.set_tracer_provider(trace_provider)

    if os.getenv('OTEL_LOG_LEVEL', '') == 'debug':
        metric_exporter = ConsoleMetricExporter()
    else:
        metric_exporter = OTLPMetricExporter()
    metric_reader = PeriodicExportingMetricReader(exporter=metric_exporter)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader]
    )
    metrics.set_meter_provider(meter_provider)
    SystemMetricsInstrumentor().instrument()
    
    tracer = trace.get_tracer(__name__)

    SPAN_KEY = 'span_key'
    ACTIVATION_KEY = 'activation_key'

    @app.on_request
    async def on_request(req: Request):
        context.attach(extract(req.headers))
        span = tracer.start_span(
            req.method + ' ' + (('/' + req.route.path)
                                if req.route else req.path),
            kind=trace.SpanKind.SERVER,
        )
        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()
        span.set_attribute(SpanAttributes.HTTP_METHOD, req.method)
        span.set_attribute(SpanAttributes.HTTP_ROUTE, req.path)
        req.ctx.cubeapm = {ACTIVATION_KEY: activation, SPAN_KEY: span}

    @app.on_response
    async def on_response(req: Request, res: HTTPResponse):
        if hasattr(req.ctx, 'cubeapm'):
            req.ctx.cubeapm[SPAN_KEY].set_attribute(
                SpanAttributes.HTTP_STATUS_CODE, res.status)
            req.ctx.cubeapm[ACTIVATION_KEY].__exit__(None, None, None)

    @app.signal('http.lifecycle.exception')
    async def on_exception(request:  Request, exception: Exception):
        if hasattr(request.ctx, 'cubeapm'):
            request.ctx.cubeapm[SPAN_KEY].record_exception(exception)
            request.ctx.cubeapm[SPAN_KEY].set_status(Status(StatusCode.ERROR))
