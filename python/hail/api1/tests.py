"""
Unit tests for Hail.
"""
from __future__ import print_function  # Python 2 and 3 print compatibility

import os
import random
import shutil
import unittest

from hail import HailContext, KeyTable, VariantDataset
from hail.api1.keytable import asc, desc
from hail.expr.types import *
from hail.genetics import *
from hail.utils import *

hc = None

def setUpModule():
    global hc
    hc = HailContext()  # master = 'local[2]')

def tearDownModule():
    global hc
    hc.stop()
    hc = None

def float_eq(x, y, tol=10 ** -6):
    return abs(x - y) < tol

class ContextTests(unittest.TestCase):
    def test_context(self):
        test_resources = 'src/test/resources'

        hc.grep('Mom1', test_resources + '/mendel.fam')

        # index
        hc.index_bgen(test_resources + '/example.v11.bgen')

        bgen = hc.import_bgen(test_resources + '/example.v11.bgen',
                              sample_file=test_resources + '/example.sample',
                              contig_recoding={"01": "1"})
        self.assertTrue(bgen.variants_table().forall("""v.contig == "1" """))
        self.assertEqual(bgen.count()[1], 199)

        gen = hc.import_gen(test_resources + '/example.gen',
                            sample_file=test_resources + '/example.sample',
                            contig_recoding={"01": "1"})
        self.assertTrue(gen.variants_table().forall("""v.contig == "1" """))
        self.assertEqual(gen.count()[1], 199)

        vcf = hc.import_vcf(test_resources + '/sample2.vcf',
                            reference_genome=GenomeReference.GRCh38(),
                            contig_recoding={"22": "chr22"}).split_multi_hts()

        self.assertTrue(vcf.variants_table().forall("""v.contig == "chr22" """))
        vcf.export_plink('/tmp/sample_plink')

        bfile = '/tmp/sample_plink'
        plink = hc.import_plink(
            bfile + '.bed', bfile + '.bim', bfile + '.fam', a2_reference=True, contig_recoding={'chr22': '22'})
        self.assertTrue(plink.variants_table().forall("""v.contig == "22" """))
        self.assertEqual(vcf.count(), plink.count())

        vcf.write('/tmp/sample.vds', overwrite=True)
        vds = hc.read('/tmp/sample.vds')
        self.assertTrue(vcf.same(vds))

        vcf.write('/tmp/sample.pq.vds', overwrite=True)
        self.assertTrue(vcf.same(hc.read('/tmp/sample.pq.vds')))

        bn = hc.balding_nichols_model(3, 10, 100, 8)
        bn_count = bn.count()
        self.assertEqual(bn_count[0], 10)
        self.assertEqual(bn_count[1], 100)

        self.assertEqual(hc.eval_expr_typed('[1, 2, 3].map(x => x * 2)'), ([2, 4, 6], TArray(TInt32())))
        self.assertEqual(hc.eval_expr('[1, 2, 3].map(x => x * 2)'), [2, 4, 6])

        vcf_metadata = hc.get_vcf_metadata(test_resources + '/sample.vcf.bgz')

        gds = hc.import_vcf(test_resources + '/sample.vcf.bgz')
        self.assertEqual(gds.genotype_schema, Type.hts_schema())
        gds = hc.import_vcf(test_resources + '/sample.vcf.bgz')

        gds.write('/tmp/sample_generic.vds', overwrite=True)
        gds_read = hc.read('/tmp/sample_generic.vds')
        self.assertTrue(gds.same(gds_read))

        gds.export_vcf('/tmp/sample_generic.vcf', metadata=vcf_metadata)
        gds_imported = hc.import_vcf('/tmp/sample_generic.vcf')
        self.assertTrue(gds.same(gds_imported))

        metadata_imported = hc.get_vcf_metadata('/tmp/sample_generic.vcf')
        self.assertDictEqual(vcf_metadata, metadata_imported)

        matrix = hc.import_matrix(test_resources + '/samplematrix1.txt', 'v', [TString()], annotation_headers=['v'])
        self.assertEqual(matrix.count()[1], 10)

    def test_dataset(self):
        test_resources = 'src/test/resources'

        vds = hc.import_vcf(test_resources + '/sample.vcf')
        vds2 = hc.import_vcf(test_resources + '/sample2.vcf')

        for (dataset, dataset2) in [(vds, vds2)]:
            gt = 'g.GT'

            dataset = dataset.cache()
            dataset2 = dataset2.persist()

            dataset.write('/tmp/sample.vds', overwrite=True)

            dataset.count()

            self.assertEqual(dataset.head(3).count_variants(), 3)

            dataset.query_variants(['variants.count()'])
            dataset.query_samples(['samples.count()'])

            (dataset.annotate_samples_expr('sa.nCalled = gs.filter(g => isDefined({0})).count()'.format(gt))
             .samples_table().select(['s', 'nCalled = sa.nCalled']).export('/tmp/sa.tsv'))

            dataset.annotate_global_expr('global.foo = 5')
            dataset.annotate_global_expr(['global.foo = 5', 'global.bar = 6'])

            dataset = dataset.annotate_samples_table(hc.import_table(test_resources + '/sampleAnnotations.tsv')
                                                     .key_by('Sample'),
                                                     expr='sa.isCase = table.Status == "CASE", sa.qPhen = table.qPhen')

            (dataset.annotate_variants_expr('va.nCalled = gs.filter(g => isDefined({0})).count()'.format(gt))
             .count())

            loci_tb = (hc.import_table(test_resources + '/sample2_loci.tsv')
                       .annotate('locus = Locus(chr, pos.toInt32())').key_by('locus'))
            (dataset.annotate_variants_table(loci_tb, root='va.locus_annot')
             .count())

            variants_tb = (hc.import_table(test_resources + '/variantAnnotations.tsv')
                           .annotate('variant = Variant(Chromosome, Position.toInt32(), Ref, Alt)').key_by('variant'))
            (dataset.annotate_variants_table(variants_tb, root='va.table')
             .count())

            (dataset.annotate_variants_vds(dataset, expr='va.good = va.info.AF == vds.info.AF')
             .count())

            downsampled = dataset.sample_variants(0.10)
            downsampled.variants_table().select(['chr = v.contig', 'pos = v.start']).export('/tmp/sample2_loci.tsv')
            downsampled.variants_table().select('v').export('/tmp/sample2_variants.tsv')

            with open(test_resources + '/sample2.sample_list') as f:
                samples = [s.strip() for s in f]
            (dataset.filter_samples_list(samples)
             .count()[0] == 56)

            locus_tb = (hc.import_table(test_resources + '/sample2_loci.tsv')
                        .annotate('locus = Locus(chr, pos.toInt32())')
                        .key_by('locus'))

            (dataset.annotate_variants_table(locus_tb, root='va.locus_annot').count())

            tb = (hc.import_table(test_resources + '/variantAnnotations.tsv')
                  .annotate('variant = Variant(Chromosome, Position.toInt32(), Ref, Alt)')
                  .key_by('variant'))
            (dataset.annotate_variants_table(tb, root='va.table').count())

            (dataset.annotate_variants_vds(dataset, expr='va.good = va.info.AF == vds.info.AF').count())

            dataset.export_vcf('/tmp/sample2.vcf.bgz')

            self.assertEqual(dataset.drop_samples().count()[0], 0)
            self.assertEqual(dataset.drop_variants().count()[1], 0)

            dataset_dedup = (hc.import_vcf([test_resources + '/sample2.vcf',
                                            test_resources + '/sample2.vcf'])
                             .deduplicate())
            self.assertEqual(dataset_dedup.count()[1], 735)

            (dataset.filter_samples_expr('pcoin(0.5)')
             .samples_table().select('s').export('/tmp/sample2.sample_list'))

            (dataset.filter_variants_expr('pcoin(0.5)')
             .variants_table().select('v').export('/tmp/sample2.variant_list'))

            (dataset.filter_variants_table(
                KeyTable.import_interval_list(test_resources + '/annotinterall.interval_list'))
             .count())

            dataset.filter_intervals(Interval.parse('1:100-end')).count()
            dataset.filter_intervals(map(Interval.parse, ['1:100-end', '3-22'])).count()

            (dataset.filter_variants_table(
                KeyTable.import_interval_list(test_resources + '/annotinterall.interval_list'))
             .count())

            self.assertEqual(dataset2.filter_variants_table(
                hc.import_table(test_resources + '/sample2_variants.tsv',
                                key='f0', impute=True, no_header=True))
                             .count()[1], 21)

            m2 = {r.f0: r.f1 for r in hc.import_table(test_resources + '/sample2_rename.tsv',
                                                      no_header=True).collect()}
            self.assertEqual(dataset2.join(dataset2.rename_samples(m2))
                             .count()[0], 200)

            dataset._typecheck()

            dataset.variants_table().export('/tmp/variants.tsv')
            self.assertTrue((dataset.variants_table()
                             .annotate('va = json(va)'))
                            .same(hc.import_table('/tmp/variants.tsv', impute=True).key_by('v')))

            dataset.samples_table().export('/tmp/samples.tsv')
            self.assertTrue((dataset.samples_table()
                             .annotate('s = s, sa = json(sa)'))
                            .same(hc.import_table('/tmp/samples.tsv', impute=True).key_by('s')))

            gt_string = 'gt = g.GT, gq = g.GQ'
            gt_string2 = 'gt: g.GT, gq: g.GQ'

            cols = ['v = v', 'info = va.info']
            for s in dataset.sample_ids:
                cols.append('`{s}`.gt = va.G["{s}"].gt'.format(s=s))
                cols.append('`{s}`.gq = va.G["{s}"].gq'.format(s=s))

            dataset_table = (dataset
             .annotate_variants_expr('va.G = index(gs.map(g => { s: s, %s }).collect(), s)' % gt_string2)
             .variants_table().select(cols))

            dataset_table_typs = {fd.name: fd.typ for fd in dataset_table.schema.fields}
            dataset_table.export('/tmp/sample_kt.tsv')

            self.assertTrue((dataset
              .make_table('v = v, info = va.info', gt_string, ['v']))
             .same(hc.import_table('/tmp/sample_kt.tsv', types=dataset_table_typs).key_by('v')))

            dataset.annotate_variants_expr("va.nHet = gs.filter(g => {0}.isHet()).count()".format(gt))

            dataset.make_table('v = v, info = va.info', 'gt = {0}'.format(gt), ['v'])

            dataset.num_partitions()
            dataset.file_version()
            dataset.sample_ids[:5]
            dataset.variant_schema
            dataset.sample_schema

            self.assertEqual(dataset2.num_samples, 100)
            self.assertEqual(dataset2.count_variants(), 735)

            dataset.annotate_variants_table(dataset.variants_table(), root="va")

            kt = (dataset.variants_table()
                  .annotate("v2 = v")
                  .key_by(["v", "v2"]))

            dataset.annotate_variants_table(kt, root='va.foo', vds_key=["v", "v"])

            self.assertEqual(kt.query('v.fraction(x => x == v2)'), 1.0)

            dataset.genotypes_table()

            ## This is very slow!!!
            variants_py = (dataset
                           .annotate_variants_expr('va.hets = gs.filter(g => {0}.isHet()).collect()'.format(gt))
                           .variants_table()
                           .filter('pcoin(0.1)')
                           .collect())

            expr = 'g.GT.isHet() && g.GQ > 20'

            (dataset.filter_genotypes(expr)
             .genotypes_table()
             .select(['v', 's', 'nNonRefAlleles = {0}.nNonRefAlleles()'.format(gt)])
             .export('/tmp/sample2_genotypes.tsv'))

            self.assertTrue(
                (dataset.repartition(16, shuffle=False)
                 .same(dataset)))

            self.assertTrue(dataset.naive_coalesce(2).same(dataset))

            print(dataset.storage_level())

            dataset = dataset.unpersist()
            dataset2 = dataset2.unpersist()

            new_sample_order = dataset.sample_ids[:]
            random.shuffle(new_sample_order)
            self.assertEqual(vds.reorder_samples(new_sample_order).sample_ids, new_sample_order)

        sample = hc.import_vcf(test_resources + '/sample.vcf').cache()

        sample.summarize().report()
        sample.drop_samples().summarize().report()

        sample_split = sample.split_multi_hts()

        sample2 = hc.import_vcf(test_resources + '/sample2.vcf')
        sample2 = sample2.persist()

        sample2_split = sample2.split_multi_hts()

        sample.annotate_alleles_expr_hts('va.gs = gs.map(g => g.GT).callStats(g => v)').count()
        sample.annotate_alleles_expr_hts(['va.gs = gs.map(g => g.GT).callStats(g => v)', 'va.foo = 5']).count()

        glob, concordance1, concordance2 = (sample2_split.concordance(sample2_split))
        print(glob[1][4])
        print(glob[4][0])
        print(glob[:][3])
        concordance1.write('/tmp/foo.kt', overwrite=True)
        concordance2.write('/tmp/foo.kt', overwrite=True)

        sample2_split.export_gen('/tmp/sample2.gen', 5)
        sample2_split.export_plink('/tmp/sample2')

        sample2.filter_variants_expr('v.isBiallelic').count()

        sample2.split_multi_hts().grm().export_gcta_grm_bin('/tmp/sample2.grm')

        sample2.hardcalls().count()

        sample2_split.ibd(min=0.2, max=0.6)

        sample2.split_multi_hts().impute_sex().variant_schema

        self.assertEqual(sample2.genotype_schema, Type.hts_schema())

        m2 = {r.f0: r.f1 for r in hc.import_table(test_resources + '/sample2_rename.tsv', no_header=True,
                                                  impute=True)
            .collect()}
        self.assertEqual(sample2.join(sample2.rename_samples(m2))
                         .count()[0], 200)

        cov = hc.import_table(test_resources + '/regressionLinear.cov',
                              types={'Cov1': TFloat64(), 'Cov2': TFloat64()}).key_by('Sample')

        phen1 = hc.import_table(test_resources + '/regressionLinear.pheno', missing='0',
                                types={'Pheno': TFloat64()}).key_by('Sample')
        phen2 = hc.import_table(test_resources + '/regressionLogisticBoolean.pheno', missing='0',
                                types={'isCase': TBoolean()}).key_by('Sample')

        regression = (hc.import_vcf(test_resources + '/regressionLinear.vcf')
                      .split_multi_hts()
                      .annotate_samples_table(cov, root='sa.cov')
                      .annotate_samples_table(phen1, root='sa.pheno.Pheno')
                      .annotate_samples_table(phen2, root='sa.pheno.isCase'))

        (regression.linreg(['sa.pheno.Pheno'], 'g.GT.nNonRefAlleles()', covariates=['sa.cov.Cov1', 'sa.cov.Cov2 + 1 - 1'])
         .count())

        (regression.logreg('wald', 'sa.pheno.isCase', 'g.GT.nNonRefAlleles()', covariates=['sa.cov.Cov1', 'sa.cov.Cov2 + 1 - 1'])
         .count())

        vds_assoc = (regression
                     .annotate_samples_expr(
                         'sa.culprit = gs.filter(g => v == Variant("1", 1, "C", "T")).map(g => g.GT.gt).collect()[0]')
                     .annotate_samples_expr('sa.pheno.PhenoLMM = (1 + 0.1 * sa.cov.Cov1 * sa.cov.Cov2) * sa.culprit'))

        covariatesSkat = hc.import_table(test_resources + "/skat.cov", impute=True).key_by("Sample")

        phenotypesSkat = (hc.import_table(test_resources + "/skat.pheno", types={"Pheno": TFloat64()}, missing="0")
                          .key_by("Sample"))

        intervalsSkat = KeyTable.import_interval_list(test_resources + "/skat.interval_list")

        weightsSkat = (hc.import_table(test_resources + "/skat.weights",
                                       types={"locus": TLocus(), "weight": TFloat64()})
                       .key_by("locus"))

        skatVds = (vds2.split_multi_hts()
            .annotate_variants_table(intervalsSkat, root="va.gene")
            .annotate_variants_table(weightsSkat, root="va.weight")
            .annotate_samples_table(phenotypesSkat, root="sa.pheno")
            .annotate_samples_table(covariatesSkat, root="sa.cov")
            .annotate_samples_expr("sa.pheno = if (sa.pheno == 1.0) false else " +
                                   "if (sa.pheno == 2.0) true else NA: Boolean"))

        (skatVds.skat(key_expr='va.gene',
                      weight_expr='va.weight',
                      y='sa.pheno',
                      x='g.GT.nNonRefAlleles()',
                      covariates=['sa.cov.Cov1', 'sa.cov.Cov2'],
                      logistic=False).count())

        (skatVds.skat(key_expr='va.gene',
                      weight_expr='va.weight',
                      y='sa.pheno',
                      x='plDosage(g.PL)',
                      covariates=['sa.cov.Cov1', 'sa.cov.Cov2'],
                      logistic=True).count())

        vds_kinship = vds_assoc.filter_variants_expr('v.start < 4')

        km = vds_kinship.rrm(False, False)

        ld_matrix_path = '/tmp/ldmatrix'
        ldMatrix = vds_kinship.ld_matrix()
        if os.path.isdir(ld_matrix_path):
            shutil.rmtree(ld_matrix_path)
        ldMatrix.write(ld_matrix_path)
        LDMatrix.read(ld_matrix_path).to_local_matrix()

        vds_assoc = vds_assoc.lmmreg(km, 'sa.pheno.PhenoLMM', 'g.GT.nNonRefAlleles()', ['sa.cov.Cov1', 'sa.cov.Cov2'])

        vds_assoc.variants_table().select(['Variant = v', 'va.lmmreg.*']).export('/tmp/lmmreg.tsv')

        men, fam, ind, var = sample_split.mendel_errors(Pedigree.read(test_resources + '/sample.fam'))
        men.select(['fid', 's', 'code'])
        fam.select(['father', 'nChildren'])
        self.assertEqual(ind.key, ['s'])
        self.assertEqual(var.key, ['v'])
        sample_split.annotate_variants_table(var, root='va.mendel').count()

        sample_split.pca_of_normalized_genotypes()

        sample_split.tdt(Pedigree.read(test_resources + '/sample.fam'))

        sample2_split.variant_qc().variant_schema

        sample2.variants_table().export('/tmp/variants.tsv')
        self.assertTrue((sample2.variants_table()
                         .annotate('va = json(va)'))
                        .same(hc.import_table('/tmp/variants.tsv', impute=True).key_by('v')))

        sample2.samples_table().export('/tmp/samples.tsv')
        self.assertTrue((sample2.samples_table()
                         .annotate('s = s, sa = json(sa)'))
                        .same(hc.import_table('/tmp/samples.tsv', impute=True).key_by('s')))

        cols = ['v = v', 'info = va.info']
        for s in sample2.sample_ids:
            cols.append('{s}.gt = va.G["{s}"].gt'.format(s=s))
            cols.append('{s}.gq = va.G["{s}"].gq'.format(s=s))

        sample2_table = (sample2
         .annotate_variants_expr('va.G = index(gs.map(g => { s: s, gt: g.GT, gq: g.GQ }).collect(), s)')
         .variants_table().select(cols))

        sample2_table.export('/tmp/sample_kt.tsv')
        sample2_typs = {fd.name: fd.typ for fd in sample2_table.schema.fields}

        self.assertTrue((sample2
          .make_table('v = v, info = va.info', 'gt = g.GT, gq = g.GQ', ['v']))
         .same(hc.import_table('/tmp/sample_kt.tsv', types=sample2_typs).key_by('v')))

        sample_split.annotate_variants_expr("va.nHet = gs.filter(g => g.GT.isHet()).count()")

        sample2.make_table('v = v, info = va.info', 'gt = g.GT', ['v'])

        sample.num_partitions()
        sample.file_version()
        sample.sample_ids[:5]

        sample2.filter_alleles_hts('pcoin(0.5)')

        sample_split.ld_prune(8).variants_table().select('v').export("/tmp/testLDPrune.tsv")
        kt = (sample2.variants_table()
              .annotate("v2 = v")
              .key_by(["v", "v2"]))
        sample2.annotate_variants_table(kt, root="va.foo", vds_key=["v", "v"])

        self.assertEqual(kt.query('v.fraction(x => x == v2)'), 1.0)

        variants_py = (sample
                       .annotate_variants_expr('va.hets = gs.filter(g => g.GT.isHet).collect()')
                       .variants_table()
                       .take(5))

        VariantDataset.from_table(sample.variants_table())

    def test_keytable(self):
        test_resources = 'src/test/resources'

        # Import
        # columns: Sample Status qPhen
        kt = hc.import_table(test_resources + '/sampleAnnotations.tsv', impute=True).key_by('Sample')
        kt2 = hc.import_table(test_resources + '/sampleAnnotations2.tsv', impute=True).key_by('Sample')

        # Variables
        self.assertEqual(kt.num_columns, 3)
        self.assertEqual(kt.key[0], "Sample")
        self.assertEqual(kt.columns[2], "qPhen")
        self.assertEqual(kt.count(), 100)
        kt.schema

        # Export
        kt.export('/tmp/testExportKT.tsv')

        # Filter, Same
        ktcase = kt.filter('Status == "CASE"', True)
        ktcase2 = kt.filter('Status == "CTRL"', False)
        self.assertTrue(ktcase.same(ktcase2))

        # Annotate
        (kt.annotate('X = Status')
         .count())

        # Join
        kt.join(kt2, 'left').count()

        # AggregateByKey
        (kt.aggregate_by_key("Status = Status", "Sum = qPhen.sum()")
         .count())

        # Forall, Exists
        self.assertFalse(kt.forall('Status == "CASE"'))
        self.assertTrue(kt.exists('Status == "CASE"'))

        kt.rename({"Sample": "ID"})
        kt.rename(["Field1", "Field2", "Field3"])
        kt.rename([name + "_a" for name in kt.columns])

        kt.select("Sample")
        kt.select(["Sample", "Status"], qualified_name=True)

        kt.drop("Sample")
        kt.drop(["Sample", "Status"])

        kt.key_by(['Sample', 'Status'])
        kt.key_by([])

        kt.flatten()
        kt.expand_types()

        kt.to_dataframe().count()

        kt.show(10)
        kt.show(4, print_types=False, truncate_to=15)

        kt.annotate("newField = [0, 1, 2]").explode(["newField"])

        sample = hc.import_vcf(test_resources + '/sample.vcf')
        sample_variants = (sample.variants_table()
                           .annotate('v = str(v), va.filters = va.filters.toArray()')
                           .flatten())

        sample_variants2 = KeyTable.from_dataframe(sample_variants.to_dataframe()).key_by('v')
        self.assertTrue(sample_variants.same(sample_variants2))

        # cseed: calculated by hand using sort -n -k 3,3 and inspection
        self.assertTrue(kt.filter('qPhen < 10000').count() == 23)

        kt.write('/tmp/sampleAnnotations.kt', overwrite=True)
        kt3 = hc.read_table('/tmp/sampleAnnotations.kt')
        self.assertTrue(kt.same(kt3))

        # test order_by
        schema = TStruct(['a', 'b'], [TInt32(), TString()])
        rows = [{'a': 5},
                {'a': 5, 'b': 'quam'},
                {'a': -1, 'b': 'quam'},
                {'b': 'foo'},
                {'a': 7, 'b': 'baz'}]
        kt4 = KeyTable.parallelize(rows, schema, num_partitions=3)

        bya = [r.get('a') for r in kt4.order_by('a').collect()]
        self.assertEqual(bya, [-1, 5, 5, 7, None])

        bydesca = [r.get('a') for r in kt4.order_by(desc('a')).collect()]
        self.assertEqual(bydesca, [7, 5, 5, -1, None])

        byab = [(r.get('a'), r.get('b')) for r in kt4.order_by('a', 'b').collect()]
        self.assertEqual(byab, [(-1, 'quam'), (5, 'quam'), (5, None), (7, 'baz'), (None, 'foo')])

        bydescab = [(r.get('a'), r.get('b'))
                    for r in kt4.order_by(desc('a'), 'b').collect()]
        self.assertEqual(bydescab, [(7, 'baz'), (5, 'quam'), (5, None), (-1, 'quam'), (None, 'foo')])

        KeyTable.import_fam(test_resources + '/sample.fam')._typecheck()

        self.assertEqual(kt.union(kt).count(), kt.count() * 2)
        self.assertEqual(kt.union(kt, kt).count(), kt.count() * 3)

        first3 = kt.take(3)
        self.assertEqual(first3[0].qPhen, 27704)
        self.assertEqual(first3[1].qPhen, 16636)
        self.assertEqual(first3[2].qPhen, 7256)
        self.assertEqual(first3[0].Sample, 'HG00096')
        self.assertEqual(first3[1].Sample, 'HG00097')
        self.assertEqual(first3[2].Sample, 'HG00099')
        self.assertTrue(all(x.Status == 'CASE' for x in first3))

        self.assertTrue(kt.head(3).count(), 3)

        self.assertEqual(range(10), [x.index for x in KeyTable.range(10).collect()])
        self.assertTrue(KeyTable.range(200).indexed('foo').forall('index == foo'))

        kt3 = KeyTable.parallelize([{'A': Struct(c1=5, c2=21)}],
                                   TStruct(['A'], [TStruct(['c1', 'c2'], [TInt32(), TInt32()])]))

        self.assertTrue(kt3.ungroup('A').group('A', 'c1', 'c2').same(kt3))

    def test_query(self):
        vds = hc.import_vcf('src/test/resources/sample.vcf').split_multi_hts().sample_qc()

        self.assertEqual(vds.sample_ids, vds.query_samples(['samples.collect()'])[0])

    def test_annotate_global(self):
        vds = hc.import_vcf('src/test/resources/sample.vcf')

        path = 'global.annotation'

        i1 = 5
        i2 = None
        itype = TInt32()
        self.assertEqual(vds.annotate_global(path, i1, itype).globals.annotation, i1)
        self.assertEqual(vds.annotate_global(path, i2, itype).globals.annotation, i2)

        l1 = 5L
        l2 = None
        ltype = TInt64()
        self.assertEqual(vds.annotate_global(path, l1, ltype).globals.annotation, l1)
        self.assertEqual(vds.annotate_global(path, l2, ltype).globals.annotation, l2)

        # FIXME add these back in when we update py4j
        # f1 = float(5)
        # f2 = None
        # ftype = TFloat32()
        # self.assertEqual(vds.annotate_global_py(path, f1, ftype).globals.annotation, f1)
        # self.assertEqual(vds.annotate_global_py(path, f2, ftype).globals.annotation, f2)

        d1 = float(5)
        d2 = None
        dtype = TFloat64()
        self.assertEqual(vds.annotate_global(path, d1, dtype).globals.annotation, d1)
        self.assertEqual(vds.annotate_global(path, d2, dtype).globals.annotation, d2)

        b1 = True
        b2 = None
        btype = TBoolean()
        self.assertEqual(vds.annotate_global(path, b1, btype).globals.annotation, b1)
        self.assertEqual(vds.annotate_global(path, b2, btype).globals.annotation, b2)

        arr1 = [1, 2, 3, 4]
        arr2 = [1, 2, None, 4]
        arr3 = None
        arr4 = []
        arrtype = TArray(TInt32())
        self.assertEqual(vds.annotate_global(path, arr1, arrtype).globals.annotation, arr1)
        self.assertEqual(vds.annotate_global(path, arr2, arrtype).globals.annotation, arr2)
        self.assertEqual(vds.annotate_global(path, arr3, arrtype).globals.annotation, arr3)
        self.assertEqual(vds.annotate_global(path, arr4, arrtype).globals.annotation, arr4)

        set1 = {1, 2, 3, 4}
        set2 = {1, 2, None, 4}
        set3 = None
        set4 = set()
        settype = TSet(TInt32())
        self.assertEqual(vds.annotate_global(path, set1, settype).globals.annotation, set1)
        self.assertEqual(vds.annotate_global(path, set2, settype).globals.annotation, set2)
        self.assertEqual(vds.annotate_global(path, set3, settype).globals.annotation, set3)
        self.assertEqual(vds.annotate_global(path, set4, settype).globals.annotation, set4)

        dict1 = {'a': 'foo', 'b': 'bar'}
        dict2 = {'a': None, 'b': 'bar'}
        dict3 = None
        dict4 = dict()
        dicttype = TDict(TString(), TString())
        self.assertEqual(vds.annotate_global(path, dict1, dicttype).globals.annotation, dict1)
        self.assertEqual(vds.annotate_global(path, dict2, dicttype).globals.annotation, dict2)
        self.assertEqual(vds.annotate_global(path, dict3, dicttype).globals.annotation, dict3)
        self.assertEqual(vds.annotate_global(path, dict4, dicttype).globals.annotation, dict4)

        map5 = {Locus("1", 100): 5, Locus("5", 205): 100}
        map6 = None
        map7 = dict()
        map8 = {Locus("1", 100): None, Locus("5", 205): 100}
        maptype2 = TDict(TLocus(), TInt32())
        self.assertEqual(vds.annotate_global(path, map5, maptype2).globals.annotation, map5)
        self.assertEqual(vds.annotate_global(path, map6, maptype2).globals.annotation, map6)
        self.assertEqual(vds.annotate_global(path, map7, maptype2).globals.annotation, map7)
        self.assertEqual(vds.annotate_global(path, map8, maptype2).globals.annotation, map8)

        struct1 = Struct(field1=5, field2=10, field3=[1, 2])
        struct2 = Struct(field1=5, field2=None, field3=None)
        struct3 = None
        structtype = TStruct(['field1', 'field2', 'field3'], [TInt32(), TInt32(), TArray(TInt32())])
        self.assertEqual(vds.annotate_global(path, struct1, structtype).globals.annotation, struct1)
        self.assertEqual(vds.annotate_global(path, struct2, structtype).globals.annotation, struct2)
        self.assertEqual(vds.annotate_global(path, struct3, structtype).globals.annotation, struct3)

        variant1 = Variant.parse('1:1:A:T,C')
        variant2 = None
        varianttype = TVariant()
        self.assertEqual(vds.annotate_global(path, variant1, varianttype).globals.annotation, variant1)
        self.assertEqual(vds.annotate_global(path, variant2, varianttype).globals.annotation, variant2)

        altallele1 = AltAllele('T', 'C')
        altallele2 = None
        altalleletype = TAltAllele()
        self.assertEqual(vds.annotate_global(path, altallele1, altalleletype).globals.annotation, altallele1)
        self.assertEqual(vds.annotate_global(path, altallele2, altalleletype).globals.annotation, altallele2)

        locus1 = Locus.parse('1:100')
        locus2 = None
        locustype = TLocus()
        self.assertEqual(vds.annotate_global(path, locus1, locustype).globals.annotation, locus1)
        self.assertEqual(vds.annotate_global(path, locus2, locustype).globals.annotation, locus2)

        interval1 = Interval.parse('1:1-100')
        interval2 = None
        intervaltype = TInterval(TLocus())
        self.assertEqual(vds.annotate_global(path, interval1, intervaltype).globals.annotation, interval1)
        self.assertEqual(vds.annotate_global(path, interval2, intervaltype).globals.annotation, interval2)

    def test_concordance(self):
        bn1 = hc.balding_nichols_model(3, 1, 50, 1, seed=10)
        bn2 = hc.balding_nichols_model(3, 1, 50, 1, seed=50)

        glob, samples, variants = bn1.concordance(bn2)
        self.assertEqual(samples.collect()[0].concordance, glob)

    def test_rename_duplicates(self):
        vds = hc.import_vcf('src/test/resources/duplicate_ids.vcf')
        vds = vds.rename_duplicates()
        duplicate_sample_ids = vds.query_samples('samples.filter(s => s != sa.originalID).map(s => sa.originalID).collectAsSet()')
        self.assertEqual(duplicate_sample_ids, {'5', '10'})

    def test_take_collect(self):
        vds = hc.import_vcf('src/test/resources/sample2.vcf')

        # need tiny example because this stuff is really slow
        n_samples = 5
        n_variants = 3
        vds = vds.filter_samples_list(vds.sample_ids[-n_samples:])
        sample_ids = vds.sample_ids

        self.assertEqual([(x.v, x.va) for x in vds.take(n_variants)],
                         [(x.v, x.va) for x in vds.variants_table().take(n_variants)])
        from_kt = {(x.v, x.s) : x.g for x in vds.genotypes_table().take(n_variants * n_samples)}
        from_vds = {(x.v, sample_ids[i]) : x.gs[i] for i in range(n_samples) for x in vds.take(n_variants)}
        self.assertEqual(from_kt, from_vds)

        vds = vds.filter_variants_expr('v.start % 199 == 0')
        self.assertEqual([(x.v, x.va) for x in vds.collect()], [(x.v, x.va) for x in vds.variants_table().collect()])
        from_kt = {(x.v, x.s) : x.g for x in vds.genotypes_table().collect()}
        from_vds = {(x.v, sample_ids[i]) : x.gs[i] for i in range(n_samples) for x in vds.collect()}
        self.assertEqual(from_kt, from_vds)

    def test_union(self):
        vds = hc.import_vcf('src/test/resources/sample2.vcf')
        vds_1 = vds.filter_variants_expr('v.start % 2 == 1')
        vds_2 = vds.filter_variants_expr('v.start % 2 == 0')

        vdses = [vds_1, vds_2]
        r1 = vds_1.union(vds_2)
        r2 = vdses[0].union(*vdses[1:])
        r3 = VariantDataset.union(*vdses)

        self.assertTrue(r1.same(r2))
        self.assertTrue(r1.same(r3))
        self.assertTrue(r1.same(vds))

    def test_trio_matrix(self):
        """
        This test depends on certain properties of the trio matrix VCF
        and pedigree structure.

        This test is NOT a valid test if the pedigree includes quads:
        the trio_matrix method will duplicate the parents appropriately,
        but the genotypes_table and samples_table orthogonal paths would
        require another duplication/explode that we haven't written.
        """
        ped = Pedigree.read('src/test/resources/triomatrix.fam')
        famkt = KeyTable.import_fam('src/test/resources/triomatrix.fam')

        vds = hc.import_vcf('src/test/resources/triomatrix.vcf')\
                .annotate_samples_table(famkt, root='sa.fam')

        dads = famkt.filter('isDefined(patID)')\
                    .annotate('isDad = true')\
                    .select(['patID', 'isDad'])\
                    .key_by('patID')

        moms = famkt.filter('isDefined(matID)') \
            .annotate('isMom = true') \
            .select(['matID', 'isMom']) \
            .key_by('matID')

        # test genotypes
        gkt = (vds.genotypes_table()
                  .key_by('s')
                  .join(dads, how='left')
                  .join(moms, how='left')
                  .annotate('isDad = isDefined(isDad), isMom = isDefined(isMom)')
                  .aggregate_by_key('v = v, fam = sa.fam.famID',
                                    'data = g.map(g => {role: if (isDad) 1 else if (isMom) 2 else 0, g: g}).collect()')
                  .filter('data.length() == 3')
                  .explode('data')
                  .select(['v', 'fam', 'data']))

        tkt = (vds.trio_matrix(ped, complete_trios=True)
                  .genotypes_table()
                  .annotate('fam = sa.proband.annotations.fam.famID, data = [{role: 0, g: g.proband}, {role: 1, g: g.father}, {role: 2, g: g.mother}]')
                  .select(['v', 'fam', 'data'])
                  .explode('data')
                  .filter('isDefined(data.g)')
                  .key_by(['v', 'fam']))

        self.assertTrue(gkt.same(tkt))

        # test annotations
        g_sa = (vds.samples_table()
                   .join(dads, how='left')
                   .join(moms, how='left')
                   .annotate('isDad = isDefined(isDad), isMom = isDefined(isMom)')
                   .aggregate_by_key('fam = sa.fam.famID',
                                     'data = sa.map(sa => {role: if (isDad) 1 else if (isMom) 2 else 0, sa: sa}).collect()')
                   .filter('data.length() == 3')
                   .explode('data')
                   .select(['fam', 'data']))

        t_sa = (vds.trio_matrix(ped, complete_trios=True)
                .samples_table()
                .annotate('fam = sa.proband.annotations.fam.famID, data = [{role: 0, sa: sa.proband.annotations}, '
                          '{role: 1, sa: sa.father.annotations}, '
                          '{role: 2, sa: sa.mother.annotations}]')
                .select(['fam', 'data'])
                .explode('data')
                .filter('isDefined(data.sa)')
                .key_by(['fam']))

        self.assertTrue(g_sa.same(t_sa))

    def test_tdt(self):
        pedigree = Pedigree.read('src/test/resources/tdt.fam')
        tdt_res = (hc.import_vcf('src/test/resources/tdt.vcf', min_partitions=4)
                   .split_multi_hts()
                   .tdt(pedigree))

        truth = (hc
                 .import_table('src/test/resources/tdt_results.tsv',
                               types={'POSITION': TInt32(), 'T': TInt32(), 'U': TInt32(),
                                      'Chi2': TFloat64(), 'Pval': TFloat64()})
                 .annotate('v = Variant(CHROM, POSITION, REF, ALT)')
                 .drop(['CHROM', 'POSITION', 'REF', 'ALT'])
                 .key_by('v'))

        bad = (tdt_res
               .join(truth, how='outer')
               .filter('!(transmitted == T && '
                       'untransmitted == U && '
                       'abs(chi2 - Chi2) < 0.001 && '
                       'abs(p - Pval) < 0.001)'))
        if bad.count() != 0:
            bad.order_by(asc('v')).show()
            self.fail('Found rows in violation of the predicate (see show output)')

    def test_kt_globals(self):
        kt = KeyTable.range(10)
        kt = kt.annotate_global_expr('foo = [1,2,3]')
        kt = kt.annotate_global('bar', [4, 5, 6], TArray(TInt32()))
        self.assertEqual(kt.filter('foo.exists(x => x == index) || bar.exists(x => x == index)').count(), 6)

    def test_annotate_alleles_expr(self):
        paths = ['src/test/resources/sample.vcf',
                 'src/test/resources/multipleChromosomes.vcf',
                 'src/test/resources/sample2.vcf']
        for path in paths:
            vds = hc.import_vcf(path)
            vds = vds.annotate_alleles_expr_hts('''
va.gqMean = gs.map(g => g.GQ).stats().mean,
va.AC = gs.map(g => g.GT.nNonRefAlleles()).sum()
''')
            vds = vds.annotate_variants_expr('va.callStatsAC = gs.map(g => g.GT).callStats(g => v).AC[1:]')
            
            self.assertTrue(vds.variants_table()
                            .forall('va.AC == va.callStatsAC'))
            
            split_vds = vds.split_multi_hts()
            split_vds = split_vds.annotate_variants_expr(
                'va.splitGqMean = gs.map(g => g.GQ).stats().mean')
            self.assertTrue(split_vds
                            .variants_table()
                            .forall('va.gqMean[va.aIndex - 1] == va.splitGqMean'))

    def test_filter_alelles_random(self):
        paths = ['src/test/resources/sample.vcf',
                 'src/test/resources/multipleChromosomes.vcf',
                 'src/test/resources/sample2.vcf']
        for path in paths:
            vds = hc.import_vcf(path)
            vds = vds.annotate_alleles_expr_hts('va.p = pcoin(0.2)')
            vds = vds.cache()

            n1 = vds.query_variants('variants.map(v => va.p.map(x => if (x) 1 else 0).sum()).sum()')

            n2 = (vds.filter_alleles_hts('va.p[aIndex - 1]', keep_star = True)
                  .split_multi_hts(keep_star = True)
                  .count_variants())
            self.assertEqual(n1, n2)

    def test_pc_relate(self):
        (hc.balding_nichols_model(3, 100, 100)
         .pc_relate(2, 0.05, block_size=64, statistics="phi")
         .count())

    def test_pca(self):
        eigenvalues, scores, loadings = (hc.balding_nichols_model(3, 100, 100)
                                         .pca('g.GT.nNonRefAlleles()', k=2, compute_loadings=True))
        self.assertEqual(len(eigenvalues), 2)
        self.assertTrue(isinstance(scores, KeyTable))
        self.assertEqual(scores.count(), 100)
        self.assertTrue(isinstance(loadings, KeyTable))
        self.assertEqual(loadings.count(), 100)

        _, _, loadings = (hc.balding_nichols_model(3, 100, 100)
                          .pca('g.GT.nNonRefAlleles()', k=2))
        self.assertEqual(loadings, None)
