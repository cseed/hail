#!/usr/bash

# persistent disk solr-data must exist

set -ex

VDS_PATH="gs://seqr-hail/annotated/Cohen.1kpart.vds"

CONTEXT=gke_seqr-project_us-central1-a_seqr-backend-solr-ingest

gcloud container --project "seqr-project" clusters create "seqr-backend-solr-ingest" --zone "us-central1-a" --machine-type "n1-standard-8" --image-type "COS" --disk-size "100" --scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring.write","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" --num-nodes "1" --network "default" --enable-cloud-logging --enable-cloud-monitoring

gcloud --project seqr-project container clusters get-credentials --zone us-central1-a seqr-backend-solr-ingest

gcloud dataproc clusters create seqr-backend-export-cluster --zone us-central1-a --master-machine-type n1-standard-8 --master-boot-disk-size 100 --num-workers 2 --worker-machine-type n1-standard-8 --worker-boot-disk-size 100 --num-preemptible-workers 2 --image-version 1.1 --project seqr-project --initialization-actions "gs://hail-common/hail-init.sh"

kubectl --context $CONTEXT create \
	-f kubernetes/configs/solr-ingest.yaml \
	-f kubernetes/configs/solr-service.yaml

# wait for pod
while [ "`kubectl --context $CONTEXT get pods -l name=solr -o jsonpath={.items[0].status.phase}`" != "Running" ]; do
    sleep 3
done

# debug
kubectl --context $CONTEXT get pods -o json

POD=`kubectl --context $CONTEXT get pods -l name=solr -o jsonpath={.items[0].metadata.name}`

NAMENODE=`kubectl --context $CONTEXT get pods -l name=solr -o jsonpath={.items[0].spec.nodeName}`

# make sure solr is up
while [ "`kubectl --context $CONTEXT exec -i $POD -- su -c '/usr/local/solr-6.4.2/bin/solr status' solr 1>&2; echo $?`" != "0" ]; do
    sleep 3
done

kubectl exec $POD -- su -c '/usr/local/solr-6.4.2/bin/solr delete -c seqr_noref' solr || true

kubectl exec $POD -- su -c '/usr/local/solr-6.4.2/bin/solr create_collection -c seqr_noref' solr

# HASH=`gsutil cat gs://seqr-hail/latest-hash.txt`
HASH=88f7220

JAR=hail-$HASH.jar

gcloud --project seqr-project dataproc jobs submit pyspark export-solr.py \
       --cluster seqr-backend-export-cluster \
       --files=gs://seqr-hail/$JAR \
       --py-files=gs://seqr-hail/hail-$HASH.zip \
       --properties=spark.driver.extraClassPath=./$JAR,spark.executor.extraClassPath=./$JAR \
       -- $NAMENODE $VDS_PATH

# debug
kubectl exec -i $POD -- /bin/bash -c "curl 'http://localhost:30002/solr/seqr_noref/select?indent=on&q=*:*&wt=json'"

kubectl exec $POD -- su -c '/usr/local/solr-6.4.2/bin/solr stop' solr

kubectl --context $CONTEXT delete \
	-f kubernetes/configs/solr-ingest.yaml \
	-f kubernetes/configs/solr-service.yaml

gcloud -q --project seqr-project container clusters delete --async --zone us-central1-a seqr-backend-solr-ingest

gcloud -q --project seqr-project dataproc clusters delete --async seqr-backend-export-cluster
