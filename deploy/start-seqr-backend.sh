#!/bin/bash

set -ex

CONTEXT=gke_seqr-project_us-central1-a_seqr-backend

gcloud container --project "seqr-project" clusters create "seqr-backend" --zone "us-central1-a" --machine-type "n1-standard-2" --image-type "COS" --disk-size "100" --scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring.write","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" --num-nodes "1" --network "default" --enable-cloud-logging --enable-cloud-monitoring

gcloud --project seqr-project container clusters get-credentials --zone us-central1-a seqr-backend

kubectl --context $CONTEXT create \
	-f kubernetes/configs/cassandra.yaml \
	-f kubernetes/configs/cassandra-service.yaml \
	-f kubernetes/configs/solr.yaml \
	-f kubernetes/configs/solr-service.yaml \
	-f kubernetes/configs/seqrserver.yaml \
	-f kubernetes/configs/seqrserver-service.yaml
