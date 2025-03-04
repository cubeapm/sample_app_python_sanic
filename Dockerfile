FROM ubuntu:22.04

RUN apt-get update && apt-get install -y gcc libffi-dev librdkafka1 librdkafka-dev python3-pip

WORKDIR /sanic

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD . .

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["ddtrace-run", "sanic", "server:app", "--host=0.0.0.0", "--port=8000", "--workers=4", "--debug", "--reload"]
