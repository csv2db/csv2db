#
# Since: November, 2023
# Author: gvenzl
# Name: Dockerfile
# Description: Dockerfile to build Docker image
#
# Copyright 2023 Gerald Venzl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM alpine
RUN apk --update --no-cache add python3 py3-pip freetds openssl tzdata && \
    apk --update --no-cache add -t build-dependencies \
        build-base python3-dev krb5-dev \
        openssl-dev freetds-dev musl-dev && \
    pip install csv2db && \
    pip install psycopg[binary] && \
    apk del build-dependencies && \
    rm -rf /var/cache/apk/* /tmp/*

ENTRYPOINT [ "csv2db" ]
