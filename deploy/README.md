
1.  Create cluster.

2.  Set kubectl cluster:

```
gcloud --project seqr-project container clusters get-credentials --zone <zone> <cluster>
```

3.  To deploy Cassandra:

```
kubectl create -f cassandra.yaml
create -f cassandra-service.yaml
```

Note, I only have gcloud configurations.

4.  To get the Cassandra node (to connect to the NodePort), use:

```
kubectl describe pods
```

It is under `Node:`.

5. Create Cassandra table.  On a compute instance, download and unpack
Cassandra from:

`http://archive.apache.org/dist/cassandra/3.9/apache-cassandra-3.9-bin.tar.gz`

```
CQLSH_PORT=30001 CQLSH_HOST=<node> ./apache-cassandra-3.9/bin/cqlsh -f create-test-db.cql
```

where create-test-db.cql is:

```
DROP KEYSPACE IF EXISTS test;
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE test.test (chrom text, start int, ref text, alt text, dataset_5fid text, PRIMARY KEY (chrom, start, ref, alt, dataset_5fid));
```

6.  Submit `scripts/export-cass-example.py` to Dataproc to export Cohen to Cassandra.

7. To deploy Solr:

```
kubectl create -f solr.yaml
```

8.  Test it is working.  On a compute instance, download and unpack zookeeper from:

`http://apache.mirrors.hoobly.com/zookeeper/zookeeper-3.4.10/zookeeper-3.4.10.tar.gz`

```
wget <node>:30002
./zookeeper-3.4.10/bin/zkCli.sh -server 10.128.0.2:30003
```

Instead of `wget`, you can port forward to the WebUI.

9.  Create test collection:

```
kubectl exec <solr-pod> -- su -c '/usr/local/solr-6.4.2/bin/solr create_collection -c test_noref' solr
```

10.  Export datasets to Cassandra/Solr:
`scripts/export-solr-example.py` and `export-cass-example.py`.  Adjust
the input dataset and the node.

11.  Connect to a compute instance and port forward to the Solr WebUI
to make sure the export worked:

```
gcloud --project seqr-project compute ssh --zone <zone> <instance> -- -L 1234:<node>:30002
```

You can select from the Cassandra table from `cqlsh`.

12. I haven't set up deployment for the seqrserver itself.  There's an
untested docker image, but it is for local use only as it assumes hail
has been built and mounted on /hail.

To build zip file, run `./gradlew archiveZip`.  It will be in
`build/distributions/hail-python.zip`.

to delete a pod: kubectl delete -f solr.yaml

to get a shell on a pod: kubectl exec -i -t <pod> -- /bin/bash
