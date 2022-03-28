FROM debian:sid
RUN apt-get update && apt-get -y update

RUN apt-get -y update \
    && apt-get install -y wget \
    && apt-get install -y jq \
    && apt-get install -y lsb-release \
    && apt-get install -y adoptopenjdk-8-hotspot \
    && apt-get install -y build-essential python3-pip \
    && pip3 -q install pip --upgrade \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
         /usr/share/man /usr/share/doc /usr/share/doc-base

ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_PYTHON=python3

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

EXPOSE 8501

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

COPY ./content/ /content/
WORKDIR content/

ENTRYPOINT ["/entrypoint.sh"]
