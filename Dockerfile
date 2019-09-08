FROM openjdk:8-alpine

# install nitty-gritty helper tools
RUN apk --update add wget tar bash

# install Python3 for pyspark
RUN apk add --no-cache python3 \
    && if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi

# install SPARK
RUN wget http://apache.mirror.anlx.net/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz

RUN tar -xzf spark-2.4.4-bin-hadoop2.7.tgz && \
    mv spark-2.4.4-bin-hadoop2.7 /spark && \
    rm spark-2.4.4-bin-hadoop2.7.tgz

ENV SPARK_HOME=/spark
ENV PATH=$PATH:/spark/bin

# install scripts to run different SPARK components
COPY start-master.sh /start-master.sh
COPY start-worker.sh /start-worker.sh

# make source code available
COPY structured_streaming /app
COPY README.md /
