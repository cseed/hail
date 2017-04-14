from hail.java import Env, scala_package_object, jset

def solr_client(zkhost, collection):
    seqr = scala_package_object(Env.hail().seqr)
    return seqr.solrClient(zkhost, collection)

def solr_handler(solr):
    seqr = scala_package_object(Env.hail().seqr)
    return seqr.solrHandler(solr)

def vds_handler(hc, vexpr, gexpr, path):
    seqr = scala_package_object(Env.hail().seqr)
    return seqr.vdsHandler(hc._jhc, vexpr, gexpr, path)

def compound_handler(search_engine, lookup_engine):
    return Env.hail().seqr.CompoundSeqrHandler(search_engine, lookup_engine)

def solr_search_engine(solr):
    return Env.hail().seqr.SolrSearchEngine(solr)

def vds_lookup_engine(hc, vexpr, gexpr, path):
    return Env.hail().seqr.VDSLookupEngine(hc._jhc, vexpr, gexpr, path)

def cass_lookup_engine(hc, address, keyspace, table, json_columns=[], port=None):
    return Env.hail().seqr.CassLookupEngine(hc._jhc, address, keyspace, table, jset(json_columns), port)

def run_server(handler, port=6060):
    seqr = scala_package_object(Env.hail().seqr)
    return seqr.runServer(handler, port)
