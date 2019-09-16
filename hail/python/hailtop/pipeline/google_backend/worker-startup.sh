#!/bin/bash
set -x

for i in $(seq 1 30); do
    gsutil -m cp gs://hail-common/dev2/pipeline/run-worker.sh gs://hail-common/dev2/pipeline/worker.py /
    if [[ $? = 0 ]]; then
        break;
    fi
    sleep 5
done

VERSION=1.5.0
OS=linux
ARCH=amd64
curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v${VERSION}/docker-credential-gcr_${OS}_${ARCH}-${VERSION}.tar.gz" \
  | tar xz --to-stdout ./docker-credential-gcr \
  | sudo tee /usr/bin/docker-credential-gcr > /dev/null && sudo chmod +x /usr/bin/docker-credential-gcr

docker-credential-gcr configure-docker
gcloud -q auth configure-docker

nohup /bin/bash run-worker.sh >run-worker.log 2>&1 &
