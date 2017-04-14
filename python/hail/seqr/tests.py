import time
import requests
import unittest
from subprocess import call

from hail import *
from hail.utils import *
from hail.seqr import *

hc = None
vds = None
vexprs = None
gexprs = None

solr_home = '/apache/solr-6.4.1'
cass_home = '/apache/apache-cassandra-3.9'

def escaped_export_expr(exprs):
    return ' , '.join(['{} = {}'.format(escape_identifier(e[0]), e[1])
                       for e in exprs])

def escape_sample_ids(vds):
    return vds.rename_samples({s: escape_identifier(s) for s in vds.sample_ids})

def make_solr_keytable(vds, vexprs, gexprs):
    kt = vds.make_keytable(escaped_export_expr(vexprs),
                           escaped_export_expr(gexprs),
                           separator='__')
    
    fa = {escape_identifier(e[0]): e[2]
           for e in vexprs}
    
    gfa = {s + '__' + escape_identifier(e[0]): e[2]
           for e in gexprs
           for s in vds.sample_ids}
    fa.update(gfa)
    
    return (kt, fa)

vds_path = '/tmp/sample.vds'

def export_vds(vds):    
    vds.write(vds_path, overwrite=True)

def export_solr_all(vds):
    call([solr_home + '/bin/solr', 'delete', '-c', 'test_all'])
    call([solr_home + '/bin/solr', 'create_collection', '-c', 'test_all'])
    
    solr_kt = (escape_sample_ids(vds)
               .make_keytable(vexprs, gexprs, separator='__'))
    solr_kt.export_solr('localhost:9983', 'test_all')

def export_solr_noref(vds):
    call([solr_home + '/bin/solr', 'delete', '-c', 'test_noref'])
    call([solr_home + '/bin/solr', 'create_collection', '-c', 'test_noref'])
    
    stored = {'docValues': False}
    indexed = {'stored': False, 'docValues': False}
    
    (solr_kt, field_attrs) = make_solr_keytable(
        escape_sample_ids(vds),
        [('chrom', 'v.contig', stored),
         ('start', 'v.start', stored),
         ('ref', 'v.ref', stored),
         ('alt', 'v.alt', stored),
         ('dataset_id', '"sample"', stored),
         ('AC', 'va.info.AC[0]', indexed),
         ('AN', 'va.info.AN', indexed),
         ('AF', 'va.info.AF[0]', indexed),
         ('culprit', 'va.info.culprit', indexed)],
        [('num_alt', 'let n = g.nNonRefAlleles.orElse(-1) in if (n != 0) n else NA: Int', indexed),
         ('gq', 'if (g.nNonRefAlleles > 0) g.gq else NA: Int', indexed)])
    
    solr_kt.export_solr('localhost:9983', 'test_noref', field_attrs)

def export_cass(vds):
    call([cass_home + '/bin/cqlsh', '-f', 'python/hail/seqr/create-test-db.cql'])
    
    cass_kt = (escape_sample_ids(vds)
               .make_keytable(vexprs, gexprs, separator='__'))
    cass_kt.export_cassandra('localhost', 'test', 'test')

def setUpModule():
    global hc, vds, vexprs, gexprs
    
    hc = HailContext(min_block_size=0) # master = 'local[2]')
    
    vexprs = escaped_export_expr([
        ('chrom', 'v.contig'),
        ('start', 'v.start'),
        ('ref', 'v.ref'),
        ('alt', 'v.alt'),
        ('dataset_id', '"sample"'),
        ('AC', 'va.info.AC[0]'),
        ('AN', 'va.info.AN'),
        ('AF', 'va.info.AF[0]'),
        ('culprit', 'va.info.culprit')])
    
    gexprs = escaped_export_expr([
        ('num_alt', 'g.nNonRefAlleles'),
        ('gq', 'g.gq')])
    
    vds = hc.import_vcf('src/test/resources/sample.vcf', npartitions=8)
    
    print('export vds')
    export_vds(vds)
    
    print('export Solr all')
    export_solr_all(vds)
    
    print('export Solr noref')
    export_solr_noref(vds)
    
    print('export Cassandra')
    export_cass(vds)

def tearDownModule():
    global hc
    hc.stop()
    hc = None

def is_unique(lst):
    return len(lst) == len(set(lst))

def response_variants(j):
    variants = [(v['chrom'], v['start'], v['ref'], v['alt']) for v in j['variants']]
    
    # variants should be unique
    assert(is_unique(variants))
    
    return variants

def response_samples(j):
    return set([ s
                 for v in j['variants']
                 for s in v.get('genotypes', {}).keys()])

def is_asc(a, b):
    return a <= b

def is_desc(a, b):
    return b <= a

def missing_last(f, a, b):
    return (a == b) or (b == None) or ((a != None) and f(a, b))

def is_sorted(xs, f):
    return all(missing_last(f, a, b) for a, b in zip(xs[:-1], xs[1:]))

def is_sorted_asc(xs):
    return is_sorted(xs, is_asc)

def is_sorted_desc(xs):
    return is_sorted(xs, is_desc)

def variants_field(j, f):
    return [v.get(f, None) for v in j['variants']]

def post(port, json):
    return requests.post('http://localhost:' + str(port), json=json)

def test_paging(port, json):
    json['limit'] = 400
    json['page'] = 1

    r = post(port, json)
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    all_variants = response_variants(j)

    assert(is_unique(all_variants))

    json['limit'] = 40
    paged_variants = []
    n = 0
    for page in range(10):
        json['page'] = page + 1

        r = post(port, json)
        assert(r.status_code == 200)
        j = r.json()
        assert(j['is_error'] == False)
        page_variants = response_variants(j)

        paged_variants = paged_variants + page_variants

    assert(is_unique(paged_variants))

    assert(set(all_variants) == set(paged_variants))

def test_handler(handler, port=6060):
    server = run_server(handler, port=port)
    time.sleep(2)

    # check all samples returned
    r = post(port, {"limit": 500, "page": 1})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 346)
    assert(response_samples(j) == set(vds.sample_ids))

    # check sort_by
    r = post(port, {
        "limit": 500,
        "page": 1,
        "sort_by": { "start": "asc" } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 346)
    assert(is_sorted_asc(variants_field(j, 'start')))

    # check sort_by
    r = post(port, {
        "limit": 500,
        "page": 1,
        "sort_by": { "start": "desc" } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 346)
    assert(is_sorted_desc(variants_field(j, 'start')))

    # check sample_ids (sample filter)
    r = post(port, {
        "limit": 500,
        "page": 1,
        "sample_ids": ["C1046::HG02024", "HG00176", "HG00146_A1"] })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 346)
    assert(response_samples(j) == {"C1046::HG02024", "HG00176", "HG00146_A1"})

    # check variant filter
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "AC": { "eq": 4 } } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    # computed with vds.filter_variants_expr('va.info.AC[0] == 4').count()
    assert(len(j['variants']) == 12)

    # check in
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "AC": { "in": [4, 6] } } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    # computed with vds.filter_variants_expr('va.info.AC[0] == 4 || va.info.AC[0] == 6').count()
    assert(len(j['variants']) == 25)

    # check in, string
    # vds.query_variants('variants.map(v => va.info.culprit).counter()')
    # 'FS': 108, 'MQRankSum': 10, 'InbreedingCoeff': 88, 'MQ': 92, 'QD': 41, 'ReadPosRankSum': 7
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "culprit": { "in": ["MQRankSum", "QD"] } } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    # computed with vds.filter_variants_expr('va.info.AC[0] == 4 || va.info.AC[0] == 6').count()
    assert(len(j['variants']) == 10 + 41)

    # check range 1
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "AC": { "range": [19, 21] } } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    # computed with vds.filter_variants_expr('va.info.AC[0] >= 19 && va.info.AC[0] <= 21').count()
    assert(len(j['variants']) == 5)

    # check range 2, lower bound
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "AC": { "range": [20, "*"] } } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    # computed with vds.filter_variants_expr('va.info.AC[0] >= 20').count()
    assert(len(j['variants']) == 105)

    # check range 3, upper bound
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "AC": { "range": ["*", 3] } } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    # computed with vds.filter_variants_expr('va.info.AC[0] <= 3').count()
    assert(len(j['variants']) == 158)

    # check range 4, unbounded
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "AC": { "range": ["*", "*"] } } })
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 346)

    # check genotype filter
    r = post(port, {
        "limit": 10,
        "page": 1,
        "variant_filters": {
            "AC": { "eq": 1 } },
        "genotype_filters": { "HG00599": {
            "num_alt": { "eq": 1 } }}})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 1)
    assert(response_variants(j) == [('20', 10579373, 'C', 'T')])

    # check genotype filter homref
    r = post(port, {
        "limit": 500,
        "page": 1,
        "genotype_filters": { "HG00599": {
            "num_alt": { "eq": 0 } }}})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 282)

    # check genotype filter homref + gq
    r = post(port, {
        "limit": 500,
        "page": 1,
        "genotype_filters": { "HG00599": {
            "num_alt": { "eq": 0 },
            "gq": { "range": [60, "*"] } }}})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 224)

    # check genotype filter homref + gq + other
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "culprit": { "in": ["FS"] } },
        "genotype_filters": { "HG00599": {
            "num_alt": { "eq": 0 },
            "gq": { "range": [60, "*"] } }}})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 76)

    # check genotype filter gt + gq on two samples
    r = post(port, {
        "limit": 500,
        "page": 1,
        "genotype_filters": {
            "HG00599": {
                "num_alt": { "eq": 0 },
                "gq": { "range": [60, "*"] } },
            "HG00100": {
                "num_alt": { "eq": 1 },
                "gq": { "range": [60, "*"] } } }})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 7)

    # check genotype filter gt + gq on two samples + other
    r = post(port, {
        "limit": 500,
        "page": 1,
        "variant_filters": {
            "AF": { "range": [0.08, "*"] } },
        "genotype_filters": {
            "HG00599": {
                "num_alt": { "eq": 0 },
                "gq": { "range": [60, "*"] } },
            "HG00100": {
        "num_alt": { "eq": 1 },
                "gq": { "range": [60, "*"] } } }})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 4)

    # check limit
    r = post(port, {"limit": 7, "page": 1})
    assert(r.status_code == 200)
    j = r.json()
    assert(j['is_error'] == False)
    assert(len(j['variants']) == 7)

    test_paging(port, {})

    print('ok!')

    server.shutdownNow()

class SeqrTests(unittest.TestCase):

    def test_vds(self):
        test_handler(
            vds_handler(hc, vexprs, gexprs, vds_path), port=6060)

    def test_solr(self):
        test_handler(
            solr_handler(solr_client('localhost:9983', 'test_all')), port=6061)


    def test_solr_vds(self):
        test_handler(compound_handler(
            solr_search_engine(solr_client('localhost:9983', 'test_noref')),
            vds_lookup_engine(hc, vexprs, gexprs, vds_path)), port=6062)

    def test_solr_cass(self):
        test_handler(compound_handler(
            solr_search_engine(solr_client('localhost:9983', 'test_noref')),
            cass_lookup_engine(hc, 'localhost', 'test', 'test')), port=6063)
