from hail import *
from hail.utils import *
from hail.seqr import *

hc = HailContext()

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

stored = {'docValues': False}
indexed = {'stored': False, 'docValues': False}

# vds = hc.read('gs://seqr-hail/annotated/Cohen.1kpart.vds')
# vds = hc.import_vcf('/Users/cseed/sample.vcf')
vds = hc.import_vcf('gs://hail-common/sample.vcf')

print(vds.variant_schema)
print(vds.global_schema)
print(vds.sample_schema)

(solr_kt, field_attrs) = make_solr_keytable(
    escape_sample_ids(vds),
    [('dataset_id', '"cohen"', stored),
     ('chrom', 'v.contig', stored),
     ('start', 'v.start', stored),
     ('ref', 'v.ref', stored),
     ('alt', 'v.alt', stored),
     ('end', 'v.start + v.ref.length - 1', indexed)],
    [('num_alt', 'let n = g.nNonRefAlleles.orElse(-1) in if (n != 0) n else NA: Int', indexed),
     ('gq', 'if (g.nNonRefAlleles > 0) g.gq else NA: Int', indexed),
     ('ab', 'if (g.nNonRefAlleles > 0) g.ad[0] / g.ad.sum else NA: Double', indexed),
     ('dp', 'if (g.nNonRefAlleles > 0) g.dp else NA: Int', indexed)])

solr_kt.export_solr('gke-seqr-backend-default-pool-88902a41-d720:31002', 'seqr_noref', field_attrs)
