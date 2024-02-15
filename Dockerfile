FROM bitnami/spark:3.3.1-debian-11-r41

USER root
WORKDIR /

COPY poetry.lock /poetry.lock
COPY pyproject.toml /pyproject.toml

RUN python -m pip install --upgrade pip \
    && python -m pip install poetry==1.2.2
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

COPY dmf/ /dmf
RUN chmod -R 644 /dmf

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

ENTRYPOINT ["/usr/bin/tini", "-s", "--", "/opt/bitnami/spark/bin/spark-submit"]
