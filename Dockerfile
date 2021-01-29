ARG CSV2DB_VERSION="dev"
FROM alpine:3.12

ENV CSV2DB_VERSION="${CSV2DB_VERSION}"

RUN apk --update --no-cache add \
    bash \
    freetds \
    mysql-client \
    postgresql-client \
    python3 \
    py3-setuptools \
    tzdata \
  && apk --update --no-cache add -t build-dependencies \
    build-base \
    freetds-dev \
    musl-dev \
    mysql-dev \
    postgresql-dev \
    py3-pip \
    python3-dev \
  && pip3 install --upgrade pip \
  && pip3 install \
    cx-Oracle \
    mysql-connector-python \
    psycopg2-binary \
    pymssql \
    ibm-db \
  && apk del build-dependencies \
  && rm -rf /var/cache/apk/* /tmp/*

WORKDIR /app
COPY src/python .

ENTRYPOINT [ "/app/csv2db.py" ]
