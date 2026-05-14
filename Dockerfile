FROM flink:1.19

RUN mkdir -p /opt/flink/usrlib

COPY build/libs/foxwatch-analytics-1.0.0.jar /opt/flink/usrlib/foxwatch-analytics.jar