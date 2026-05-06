FROM flink:1.19

# Create the usrlib directory for the Fat Jar
RUN mkdir -p /opt/flink/usrlib

# Copy your ShadowJar into the image
COPY build/libs/foxwatch-analytics-1.0.0.jar /opt/flink/usrlib/foxwatch-analytics.jar

# (Optional) If you still want your custom log4j config
COPY src/main/resources/log4j2.xml /opt/flink/conf/log4j-console.properties
