from subprocess import call
import hail
from hail.utils import escape_identifier, unescape_identifier

def escaped_export_expr(terms):
    return ' , '.join(['{} = {}'.format(escape_identifier(id), expr)
                     for id, expr in terms])

def escape_sample_ids(vds):
    return vds.rename_samples({s: escape_identifier(s) for s in vds.sample_ids})

hc = hail.HailContext(min_block_size=0)
vds = hc.import_vcf('/Users/cseed/sample.vcf', npartitions=8)

# export VDS
vds.write('/Users/cseed/sample.vds', overwrite=True)

solr_home = '/apache/solr-6.4.1'

# export Solr all
call([solr_home + '/bin/solr', 'delete', '-c', 'test_all'])
call([solr_home + '/bin/solr', 'create_collection', '-c', 'test_all'])

solr_kt = (escape_sample_ids(vds)
      .make_keytable(
          escaped_export_expr([
              ('chrom', 'v.contig'),
              ('start', 'v.start'),
              ('ref', 'v.ref'),
              ('alt', 'v.alt'),
              ('dataset_id', '"sample"'),
              ('AC', 'va.info.AC[0]'),
              ('AN', 'va.info.AN'),
              ('AF', 'va.info.AF[0]'),
              ('culprit', 'va.info.culprit')]),
          escaped_export_expr([
              ('num_alt', 'g.nNonRefAlleles'),
              ('gq', 'g.gq')]),
          separator='__'))
solr_kt.export_solr('localhost:9983', 'test_all')

# export Solr noref
call([solr_home + '/bin/solr', 'delete', '-c', 'test_noref'])
call([solr_home + '/bin/solr', 'create_collection', '-c', 'test_noref'])

solr_kt = (escape_sample_ids(vds)
      .make_keytable(
          escaped_export_expr([
              ('chrom', 'v.contig'),
              ('start', 'v.start'),
              ('ref', 'v.ref'),
              ('alt', 'v.alt'),
              ('dataset_id', '"sample"'),
              ('AC', 'va.info.AC[0]'),
              ('AN', 'va.info.AN'),
              ('AF', 'va.info.AF[0]'),
              ('culprit', 'va.info.culprit')]),
          escaped_export_expr([
              # NA => -1, ref => NA
              ('num_alt', 'let n = g.nNonRefAlleles.orElse(-1) in if (n != 0) n else NA: Int'),
              # NA for missing, ref
              ('gq', 'if (g.nNonRefAlleles > 0) g.gq else NA: Int')]),
          separator='__'))
solr_kt.export_solr('localhost:9983', 'test_noref')

# export Cass
cass_home = '/apache/apache-cassandra-3.9'

call([cass_home + '/bin/cqlsh', '-f', 'seqr.cql'])

cass_kt = (escape_sample_ids(vds)
      .make_keytable(
          escaped_export_expr([
              ('chrom', 'v.contig'),
              ('start', 'v.start'),
              ('ref', 'v.ref'),
              ('alt', 'v.alt'),
              ('dataset_id', '"sample"'),
              ('AC', 'va.info.AC[0]'),
              ('AN', 'va.info.AN'),
              ('AF', 'va.info.AF[0]'),
              ('culprit', 'va.info.culprit')]),
          escaped_export_expr([
              ('num_alt', 'g.nNonRefAlleles'),
              ('gq', 'g.gq')]),
          separator='__'))
cass_kt.export_cassandra('localhost', 'test', 'test')
