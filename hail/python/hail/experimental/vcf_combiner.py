"""A work in progress pipeline to combine (g)VCFs into an alternate format"""

import operator

import hail as hl
from hail.matrixtable import MatrixTable
from hail.expr import ArrayExpression, Expression, NumericExpression, StructExpression

REF_CALL = hl.call(0, 0, phased=False)
SHUF = None
SHUF_ARR = [0, 3, 5, 1, 4, 2, 6, 8, 7, 9]


def mappend(fun, left, right) -> Expression:
    return hl.case()\
             .when(hl.is_missing(left), right)\
             .when(hl.is_missing(right), left)\
             .default(fun(left, right))


def position(arr, pred) -> NumericExpression:
    """gets the offset of the first element in arr that returns true for pred"""
    def helper(acc, elem):
        return hl.case(missing_false=True)\
                 .when(acc.found, acc)\
                 .when(pred(elem), acc.annotate(found=True))\
                 .default(acc.annotate(ind=acc.ind + 1))
    res = arr.fold(helper, hl.struct(ind=0, found=False))
    return hl.case().when(res.found, res.ind).default(hl.null(hl.tint32))


def transform_one(mt: MatrixTable) -> MatrixTable:
    """transforms a gvcf into a form suitable for combining"""
    # mt = mt.key_rows_by('locus')
    mt = mt.annotate_entries(
        END=mt.info.END,
        PL=mt['PL'][0:],
        BaseQRankSum=mt.info['BaseQRankSum'],
        ClippingRankSum=mt.info['ClippingRankSum'],
        MQ=mt.info['MQ'],
        MQRankSum=mt.info['MQRankSum'],
        ReadPosRankSum=mt.info['ReadPosRankSum'],
    )
    # This collects all fields with median combiners into arrays so we can calculate medians
    # when needed
    mt = mt.annotate_rows(
        info=mt.info.annotate(
            SB=hl.agg.array_sum(mt.entry.SB)
        ).select(
            "DP",
            "MQ_DP",
            "QUALapprox",
            # "RAW_MQ",
            "VarDP",
            "SB",
        ))
    # NOTE until joins are improved, we only key by locus for now
    return mt.drop('SB', 'qual')


def merge_alleles(alleles) -> ArrayExpression:
    ref = hl.find(hl.is_defined, alleles)[0]
    s = hl.array(hl.set(hl.flatten(alleles)).difference(hl.set([ref, '<NON_REF>'])))
    # TODO: handle cases where there is no NON_REF
    alen = hl.len(s) + 2
    return hl.range(0, alen).map(lambda i: hl.cond(i != 0, hl.cond(i != alen-1, s[i-1], '<NON_REF>'), ref))


def shuffle_pl(pl) -> ArrayExpression:
    # shuffling is only meaningful in our case when the GT order changes
    # the order will only change when a het-non-ref allele is called,
    # at least for the data I am currently working with
    return hl.cond(pl.length() == 10,
                   hl.range(0, 10).map(lambda i: pl[SHUF[i]]),
                   pl)


def renumber_gt(format_field, old, new) -> StructExpression:
    """renumbers GTs, for example suppose that an entry has GT=2/4, corresponding to the,
       3rd and 5th elements of old, but suppose that the 3rd element of old is the 8th element
       of new, and the 5th element of old is the 2nd element of new, then this function returns
       a format struct with a GT of 2/8 and will rearrange the PL array to account for switching
       up the order of the alleles in the GT"""
    call = format_field['GT']
    al_0 = old[call[0]]
    new_ind_0 = position(new, lambda e: e == al_0)
    al_1 = old[call[1]]
    new_ind_1 = position(new, lambda e: e == al_1)
    ncall = hl.call(new_ind_0, new_ind_1, phased=False)
    flipped = new_ind_1 < new_ind_0
    fld = format_field.annotate(GT=ncall)
    return hl.cond(flipped, fld.annotate(PL=shuffle_pl(fld['PL'])), fld)
    # return hl.cond(format_field['GT'] == REF_CALL, format_field, help2(format_field, old, new))


def combine(ts):
    # pylint: disable=protected-access
    tmp = ts.annotate(
        alleles=merge_alleles(ts.data.map(lambda d: d.alleles)),
        rsid=hl.find(hl.is_defined, ts.data.map(lambda d: d.rsid)),
        filters=hl.set(hl.flatten(ts.data.map(lambda d: hl.array(d.filters)))),
        info=hl.struct(
            DP=hl.sum(ts.data.map(lambda d: d.info.DP)),
            MQ_DP=hl.sum(ts.data.map(lambda d: d.info.MQ_DP)),
            QUALapprox=hl.sum(ts.data.map(lambda d: d.info.QUALapprox)),
            # RAW_MQ=hl.sum(ts.data.map(lambda d: d.info.RAW_MQ)),
            VarDP=hl.sum(ts.data.map(lambda d: d.info.VarDP)),
            SB=hl.fold(lambda a, b: mappend(operator.add, a, b),
                       hl.null(hl.tarray(hl.tint64)),
                       ts.data.map(lambda d: d.info.SB)),
        ),
    )
    return tmp.transmute(
        __entries=hl.flatten(
            hl.range(0, hl.len(tmp.data)).map(
                lambda i: hl.cond(hl.is_missing(tmp.data[i].__entries),
                                  hl.range(0, tmp.g[i].ncol).map(lambda _: hl.null(tmp.data[i].__entries[0].dtype)),
                                  tmp.data[i].__entries.map(lambda e: renumber_gt(e, tmp.data[i].alleles, tmp.alleles)))
            )
        )
    )


def combine_vcfs_mw(mts):
    """merges vcfs using multi way join"""
    # pylint: disable=global-statement
    global SHUF
    if SHUF is None:
        SHUF = hl.array(SHUF_ARR)

    # pylint: disable=protected-access
    def localize(mt):
        return mt._localize_entries('__entries')

    cols = None
    for mt in mts:
        if cols is None:
            cols = mt.key_cols_by().cols()
        else:
            cols = cols.union(mt.key_cols_by().cols())
    ts = hl.Table._multi_way_zip_join(
        [localize(mt.annotate_globals(ncol=mt.cols().count())) for mt in mts],
        'data',
        'g')
    combined = combine(ts).drop('g')
    return combined._unlocalize_entries(cols, '__entries')

# NOTE: these are just @chrisvittal's notes on how gVCF fields are combined
#       some of it is copied from GenomicsDB's wiki.
# always missing items include MQ, HaplotypeScore, InbreedingCoeff
# items that are dropped by CombineGVCFs and so set to missing are MLEAC, MLEAF
# Notes on info aggregation, The GenomicsDB wiki says the following:
#   The following operations are supported:
#       "sum" sum over valid inputs
#       "mean"
#       "median"
#       "element_wise_sum"
#       "concatenate"
#       "move_to_FORMAT"
#       "combine_histogram"
#
#   Operations for the fields
#   QUAL: set to missing
#   INFO {
#       BaseQRankSum: median, # NOTE : move to format for combine
#       ClippingRankSum: median, # NOTE : move to format for combine
#       DP: sum
#       ExcessHet: median, # NOTE : this can also be dropped
#       MQ: median, # NOTE : move to format for combine
#       MQ_DP: sum,
#       MQ0: median,
#       MQRankSum: median, # NOTE : move to format for combine
#       QUALApprox: sum,
#       RAW_MQ: sum
#       ReadPosRankSum: median, # NOTE : move to format for combine
#       SB: elementwise sum, # NOTE: after being moved from FORMAT
#       VarDP: sum
#   }
#   FORMAT {
#       END: move from INFO
#   }
#
# The following are Truncated INFO fields for the specific VCFs this tool targets
# ##INFO=<ID=BaseQRankSum,Number=1,Type=Float>
# ##INFO=<ID=ClippingRankSum,Number=1,Type=Float>
# ##INFO=<ID=DP,Number=1,Type=Integer>
# ##INFO=<ID=END,Number=1,Type=Integer>
# ##INFO=<ID=ExcessHet,Number=1,Type=Float>
# ##INFO=<ID=MQ,Number=1,Type=Float>
# ##INFO=<ID=MQRankSum,Number=1,Type=Float>
# ##INFO=<ID=MQ_DP,Number=1,Type=Integer>
# ##INFO=<ID=QUALapprox,Number=1,Type=Integer>
# ##INFO=<ID=RAW_MQ,Number=1,Type=Float>
# ##INFO=<ID=ReadPosRankSum,Number=1,Type=Float>
# ##INFO=<ID=VarDP,Number=1,Type=Integer>
