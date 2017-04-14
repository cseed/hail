from hail import *
from hail.utils import *
from hail.seqr import *

from subprocess import call
import time

print('in seqrserver.py...')

hc = HailContext()

# FIXME hostnames need to point to solr/cassandra services
handler = compound_handler(
    solr_search_engine(solr_client('solr-svc:9983', 'test_noref')),
    cass_lookup_engine(hc, 'cassandra-svc', 'test', 'test'))
server = run_server(handler)

print('up and running!')

time.sleep(1000000)
