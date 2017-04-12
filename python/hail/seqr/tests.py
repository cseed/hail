import hail
from hail.utils import *

import time
import requests

hc = hail.HailContext()

def escaped_export_expr(terms):
    return ' , '.join(['{} = {}'.format(escape_identifier(id), expr)
                     for id, expr in terms])

def export_expr(terms):
    return ' , '.join(['{} = {}'.format(id, expr)
                     for id, expr in terms])

def escape_sample_ids(vds):
    return vds.rename_samples({s: escape_identifier(s) for s in vds.sample_ids})

vexpr = escaped_export_expr([
    ('chrom', 'v.contig'),
    ('start', 'v.start'),
    ('ref', 'v.ref'),
    ('alt', 'v.alt'),
    ('dataset_id', '"sample"'),
    ('AC', 'va.info.AC[0]'),
    ('AN', 'va.info.AN'),
    ('AF', 'va.info.AF[0]'),
    ('culprit', 'va.info.culprit')])

gexpr = escaped_export_expr([
    ('num_alt', 'g.nNonRefAlleles'),
    ('gq', 'g.gq')])

vds_path = '/Users/cseed/sample.vds'

vds = hc.import_vcf('/Users/cseed/sample.vcf')

vds_handler = vds_handler(hc, vexpr, gexpr, vds_path)
solr_handler = solr_handler(solr_client('localhost:9983', 'test_all'))
solr_vds_handler = compound_handler(
    solr_search_engine(solr_client('localhost:9983', 'test_noref')),
    vds_lookup_engine(hc, vexpr, gexpr, vds_path))
solr_cass_handler = compound_handler(
    solr_search_engine(solr_client('localhost:9983', 'test_noref')),
    cass_lookup_engine(hc, 'localhost', 'test', 'test'))

handler = solr_handler

server = run_server(handler)
time.sleep(2)

# r = requests.post('http://localhost:6060', json={"limit": 10, "page": 1, "variant_filters": { "AC": { "eq": 1 } }, "genotype_filters": { "HG00599": { "num_alt": { "eq": 1 }}}})

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

def variant_field(j, f):
    return [v.get(f, None) for v in j['variants']]

## FIXME search for ref, search for -1

# check all samples returned
r = requests.post('http://localhost:6060', json={"limit": 500, "page": 1})
assert(r.status_code == 200)
j = r.json()
assert(j['is_error'] == False)
assert(len(j['variants']) == 346)
assert(response_samples(j) == set(vds.sample_ids))

# check sort_by
r = requests.post('http://localhost:6060', json={
    "limit": 500,
    "page": 1,
    "sort_by": { "start": "asc" } })
assert(r.status_code == 200)
j = r.json()
assert(j['is_error'] == False)
assert(len(j['variants']) == 346)

# FIXME check sorted None last
# print(variant_field(j, 'start'))

# check sample_ids (sample filter)
r = requests.post('http://localhost:6060', json={
    "limit": 500,
    "page": 1,
    "sample_ids": ["C1046::HG02024", "HG00176", "HG00146_A1"] })
assert(r.status_code == 200)
j = r.json()
assert(j['is_error'] == False)
assert(len(j['variants']) == 346)
assert(response_samples(j) == {"C1046::HG02024", "HG00176", "HG00146_A1"})

# check variant filter
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
    "limit": 500,
    "page": 1,
    "variant_filters": {
        "AC": { "range": ["*", "*"] } } })
assert(r.status_code == 200)
j = r.json()
assert(j['is_error'] == False)
assert(len(j['variants']) == 346)

# check genotype filter
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
    "limit": 500,
    "page": 1,
    "genotype_filters": { "HG00599": {
        "num_alt": { "eq": 0 } }}})
assert(r.status_code == 200)
j = r.json()
assert(j['is_error'] == False)
assert(len(j['variants']) == 282)

# check genotype filter homref + gq
r = requests.post('http://localhost:6060', json={
    "limit": 500,
    "page": 1,
    "genotype_filters": { "HG00599": {
        "num_alt": { "eq": 0 },
        "gq": { "range": [60, "*"] } }}})
assert(r.status_code == 200)
j = r.json()
assert(j['is_error'] == False)
print(len(j['variants']))
assert(len(j['variants']) == 224)

# check genotype filter homref + gq + other
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={
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
r = requests.post('http://localhost:6060', json={"limit": 7, "page": 1})
assert(r.status_code == 200)
j = r.json()
assert(j['is_error'] == False)
assert(len(j['variants']) == 7)

def test_paging(json):
    json['limit'] = 400
    json['page'] = 1
    
    r = requests.post('http://localhost:6060', json=json)
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
        
        r = requests.post('http://localhost:6060', json=json)
        assert(r.status_code == 200)
        j = r.json()
        assert(j['is_error'] == False)
        page_variants = response_variants(j)
        
        paged_variants = paged_variants + page_variants
    
    assert(is_unique(paged_variants))

    assert(set(all_variants) == set(paged_variants))

test_paging({})

# FIXME check for arrays do intersection, range: any element in the range

print('ok!')

server.shutdown()
