#!/bin/bash

# 1. Build the fresh Fat Jar
./gradlew shadowJar

# 2. Build the Docker image (this is the "Copy" step now)
docker build -t hialtdev/foxwatch-analytics:latest .

# 3. Push to your local K3s image store (since Docker Hub is giving you grief)
docker save hialtdev/foxwatch-analytics:latest | sudo k3s ctr images import -

# 4. Force Kubernetes to restart the pods with the new image
kubectl rollout restart deployment/foxwatch-dropout-detector -n flink
