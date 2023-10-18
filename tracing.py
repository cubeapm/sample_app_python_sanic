# ref: https://github.com/krvajal/sanic-opentelemetry-python-example
# ref: https://dev.to/sjsadowski/honeycomb-python-and-i-an-opentelemetry-horror-story-with-a-happy-ending-3hmc

import os
from opentelemetry import trace, context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode
from sanic import HTTPResponse, Request, Sanic


def instrument_app(app: Sanic):
    provider = TracerProvider()
    if os.getenv('OTEL_LOG_LEVEL', '') == 'debug':
        processor = SimpleSpanProcessor(ConsoleSpanExporter())
    else:
        processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
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
