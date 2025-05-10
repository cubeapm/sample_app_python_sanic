import os
import requests
import socket
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from contextvars import ContextVar
from redis import Redis
from sanic import Sanic
from sanic.exceptions import SanicException
from sanic.response import json, text
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from elasticapm.contrib.sanic import ElasticAPM
# import logging
import elasticapm.contrib.sanic.utils as apm_utils

from models import User

# If using ELASTIC_APM_LOG_FILE to check agent debug logs,
# the following may need to be uncommented to see the logs.
# logging.basicConfig()


# Sanic recently changed the CookieJar interface, and that broke the Elastic agent.
# The fix has already been merged into the ElasticAPM repo, but it hasn't been released yet.
# This is a temporary patch until the next release of elastic-apm after v6.23.0.
# Ref: https://github.com/elastic/apm-agent-python/pull/2190/files#diff-b4a325057bcab10ab8ad2ff6a47422ebd34e1583e8d207a2720d123f54e07bce
def _patched_transform_response_cookie(cookies):
    """Transform the Sanic's CookieJar instance into a Normal dictionary to build the context"""
    # old sanic versions used to have an items() method
    if hasattr(cookies, "items"):
        return {k: {"value": v.value, "path": v["path"]} for k, v in cookies.items()}

    try:
        return {cookie.key: {"value": cookie.value, "path": cookie.path} for cookie in cookies.cookies}
    except KeyError:
        # cookies.cookies assumes Set-Cookie header will be there
        return {}


apm_utils._transform_response_cookie = _patched_transform_response_cookie

app = Sanic("CubeAPMSampleApp")
apm = ElasticAPM(app=app)


## begin configure mysql ##
engine = create_async_engine(os.environ['CUBE_SAMPLE_MYSQL'], echo=True)
_sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
_base_model_session_ctx = ContextVar("session")


@app.middleware("request")
async def inject_session(request):
    request.ctx.session = _sessionmaker()
    request.ctx.session_ctx_token = _base_model_session_ctx.set(
        request.ctx.session)


@app.middleware("response")
async def close_session(request, response):
    if hasattr(request.ctx, "session_ctx_token"):
        _base_model_session_ctx.reset(request.ctx.session_ctx_token)
        await request.ctx.session.close()
## end configure mysql ##


redis_conn = Redis(host='redis', port=6379, decode_responses=True)

kafka_producer = Producer({'bootstrap.servers': 'kafka:9092',
                           'client.id': socket.gethostname()})
kafka_producer.produce('sample_topic', b'raw_bytes')

kafka_consumer = Consumer(
    {'bootstrap.servers': 'kafka:9092', 'group.id': 'foo', 'auto.offset.reset': 'smallest'})


@app.get("/")
async def home(request):
    return text("Hello")


@app.get("/param/<param>")
async def param(request, param):
    return text("Got param {}".format(param))


@app.route("/exception")
async def exception(request):
    raise SanicException("Sample exception")


@app.route("/api")
async def api(request):
    requests.get('http://localhost:8000/')
    return text("API called")


@app.post("/user")
# POST method for INSERT into MySQL
# curl -X POST 'http://localhost:8000/user' -d '{"name":"foo"}'
async def create_user(request):
    session = request.ctx.session
    async with session.begin():
        user = User(name="foo")
        session.add_all([user])
    return json(user.to_dict())


@app.get("/user/<pk:int>")
async def get_user(request, pk):
    session = request.ctx.session
    async with session.begin():
        stmt = select(User).where(User.id == pk)
        result = await session.execute(stmt)
        user = result.scalar()
    if not user:
        return json({})
    return json(user.to_dict())


@app.get('/redis')
async def redis(request):
    redis_conn.set('foo', 'bar')
    return text("Redis called")


@app.get('/kafka/produce')
async def kafka_produce(request):
    kafka_producer.produce('sample_topic', b'raw_bytes')
    kafka_producer.poll(1000)
    kafka_producer.flush()
    return text("Kafka produced")


@app.get('/kafka/consume')
async def kafka_consume(request):
    kafka_consumer.subscribe(['sample_topic'])
    while True:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is None:
            print("message received None")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return text("message received EOF")
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            return text(msg.value().decode('utf-8'))
