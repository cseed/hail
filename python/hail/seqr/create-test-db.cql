
DROP KEYSPACE IF EXISTS test;
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE test.test (chrom text, start int, ref text, alt text, dataset_5fid text, PRIMARY KEY (chrom, start, ref, alt, dataset_5fid));
