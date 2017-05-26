#!/usr/bash

# persistent disk cassandra-data must exist

set -ex

VDS_PATH="gs://seqr-hail/annotated/Cohen.1kpart.vds"

gcloud container --project "seqr-project" clusters create "seqr-backend-cassandra-ingest" --zone "us-central1-a" --machine-type "n1-standard-8" --image-type "COS" --disk-size "100" --scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring.write","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" --num-nodes "1" --network "default" --enable-cloud-logging --enable-cloud-monitoring

gcloud --project seqr-project container clusters get-credentials --zone us-central1-a seqr-backend-cassandra-ingest

CONTEXT=gke_seqr-project_us-central1-a_seqr-backend-cassandra-ingest

gcloud dataproc clusters create seqr-backend-export-cluster --zone us-central1-a --master-machine-type n1-standard-8 --master-boot-disk-size 100 --num-workers 2 --worker-machine-type n1-standard-8 --worker-boot-disk-size 100 --num-preemptible-workers 2 --image-version 1.1 --project seqr-project --initialization-actions "gs://hail-common/hail-init.sh"

kubectl --context $CONTEXT create \
	-f kubernetes/configs/cassandra-ingest.yaml \
	-f kubernetes/configs/cassandra-service.yaml

# wait for pod
while [ "`kubectl --context $CONTEXT get pods -l name=cassandra -o jsonpath={.items[0].status.phase}`" != "Running" ]; do
    sleep 3
done

# debug
kubectl --context $CONTEXT get pods -o json

POD=`kubectl --context $CONTEXT get pods -l name=cassandra -o jsonpath={.items[0].metadata.name}`

NAMENODE=`kubectl --context $CONTEXT get pods -l name=cassandra -o jsonpath={.items[0].spec.nodeName}`

# make sure cassandra is up
while [ "`echo | kubectl --context $CONTEXT exec -i $POD -- cqlsh; echo $?`" != "0" ]; do
    sleep 3
done

kubectl --context $CONTEXT exec -i $POD -- cqlsh < create-db.cql

# HASH=`gsutil cat gs://seqr-hail/latest-hash.txt`
HASH=88f7220

JAR=hail-$HASH.jar

gcloud --project seqr-project dataproc jobs submit pyspark export-cass.py \
       --cluster seqr-backend-export-cluster \
       --files=gs://seqr-hail/$JAR \
       --py-files=gs://seqr-hail/hail-$HASH.zip \
       --properties=spark.driver.extraClassPath=./$JAR,spark.executor.extraClassPath=./$JAR \
       -- $NAMENODE $VDS_PATH

# debug
kubectl --context $CONTEXT exec -i $POD -- cqlsh <<EOF
select count(*) from seqr.seqr;
EOF

kubectl --context $CONTEXT delete \
	-f kubernetes/configs/cassandra-ingest.yaml \
	-f kubernetes/configs/cassandra-service.yaml

gcloud -q --project seqr-project container clusters delete --async --zone us-central1-a seqr-backend-cassandra-ingest

gcloud -q --project seqr-project dataproc clusters delete --async seqr-backend-export-cluster
