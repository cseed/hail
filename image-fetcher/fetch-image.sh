#!/bin/sh
set -ex

gcloud auth activate-service-account \
  --key-file=/secrets/gcr-pull.json

gcloud auth configure-docker

while true
do
    for image in gcr.io/$PROJECT/base:latest $(curl -sSL http://notebook/worker-image) $(curl -sSL http://notebook2/worker-image) google/cloud-sdk
    do
    	docker pull $image
    done
    sleep 360
done
