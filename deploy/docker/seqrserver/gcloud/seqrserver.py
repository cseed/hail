from hail import *
from hail.utils import *
from hail.seqr import *

from subprocess import call
import time

print('in seqrserver.py...')

hc = HailContext()

handler = compound_handler(
    solr_search_engine(solr_client('solr-svc:31002', 'seqr_noref')),
    cass_lookup_engine(hc, 'cassandra-svc', 'seqr', 'seqr'))
server = run_server(handler)

print('up and running!')

server.awaitShutdown()
