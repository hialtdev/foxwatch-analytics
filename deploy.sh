#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
# 1. Build
./gradlew clean shadowJar

# 2. Containerize
docker build -t hialtdev/foxwatch-analytics:latest .

# 3. Clean K3s cache (The "Fix")
sudo k3s ctr images rm docker.io/hialtdev/foxwatch-analytics:latest
docker save hialtdev/foxwatch-analytics:latest | sudo k3s ctr images import -

# 4. Apply Infrastructure
kubectl apply -f k8s/flink-application.yaml