import hail as hl
from .ir import register_function, register_seeded_function

def register_reference_genome_functions(rg):
    from hail.expr.types import dtype

    tvariant = dtype(f"struct{{locus:locus<{rg}>,alleles:array<str>}}")
    tinterval = dtype(f"interval<locus<{rg}>>")
    
    register_function(f"Locus({rg})", (dtype("str"),), dtype(f"locus<{rg}>"))
    register_function(f"Locus({rg})", (dtype("str"),dtype("int32"),), dtype(f"locus<{rg}>"))
    register_function(f"LocusAlleles({rg})", (dtype("str"),), tvariant)
    register_function(f"LocusInterval({rg})", (dtype("str"),), tinterval)
    register_function(f"LocusInterval({rg})", (dtype("str"),dtype("int32"),dtype("int32"),dtype("bool"),dtype("bool"),), tinterval)
    register_function(f"isValidContig({rg})", (dtype("str"),), dtype("bool"))
    register_function(f"isValidLocus({rg})", (dtype("str"),dtype("int32"),), dtype("bool"))

    register_function(f"getReferenceSequenceFromValidLocus({rg})", (dtype("str"),dtype("int32"),dtype("int32"),dtype("int32"),), dtype("str"))
    register_function(f"getReferenceSequence({rg})", (dtype("str"),dtype("int32"),dtype("int32"),dtype("int32"),), dtype("str"))

    register_function(f"globalPosToLocus({rg})", (dtype("int64"),), dtype(f"locus<{rg}>"))
    register_function(f"locusToGlobalPos({rg})", (dtype(f"locus<{rg}>"),), dtype("int64"))

def register_liftover_functions(rg, dest_rg):
    from hail.expr.types import dtype
    
    register_function(f"liftoverLocus({rg})({dest_rg})", (dtype(f"locus<{rg}>"), dtype('float64'),), dtype(f"struct{{result:locus<{dest_rg}>,is_negative_strand:bool}}"))
    register_function(f"liftoverLocusInterval({rg})({dest_rg})", (dtype(f"interval<locus<{rg}>>"), dtype('float64'),), dtype(f"struct{{result:interval<locus<{dest_rg}>>,is_negative_strand:bool}}"))

def register_functions():
    from hail.expr.types import dtype

    register_function("flatten", (dtype("array<array<?T>>"),), dtype("array<?T>"))
    register_function("difference", (dtype("set<?T>"),dtype("set<?T>"),), dtype("set<?T>"))
    register_function("median", (dtype("set<?T:numeric>"),), dtype("?T"))
    register_function("median", (dtype("array<?T:numeric>"),), dtype("?T"))
    register_function("uniqueMinIndex", (dtype("array<?T>"),), dtype("int32"))
    register_function("mean", (dtype("set<?T:numeric>"),), dtype("float64"))
    register_function("mean", (dtype("array<?T:numeric>"),), dtype("float64"))
    register_function("toFloat32", (dtype("?T:numeric"),), dtype("float32"))
    register_function("uniqueMaxIndex", (dtype("array<?T>"),), dtype("int32"))
    register_function("toSet", (dtype("array<?T>"),), dtype("set<?T>"))

    def floating_point_divide(arg_type, ret_type):
        register_function("/", (arg_type, hl.tarray(arg_type),), hl.tarray(ret_type))
        register_function("/", (hl.tarray(arg_type),arg_type), hl.tarray(ret_type))
        register_function("/", (hl.tarray(arg_type),hl.tarray(arg_type)), hl.tarray(ret_type))
    floating_point_divide(hl.tint32, hl.tfloat32)
    floating_point_divide(hl.tint64, hl.tfloat32)
    floating_point_divide(hl.tfloat32, hl.tfloat32)
    floating_point_divide(hl.tfloat64, hl.tfloat64)

    register_function("values", (dtype("dict<?key, ?value>"),), dtype("array<?value>"))
    register_function("[*:]", (dtype("array<?T>"),dtype("int32"),), dtype("array<?T>"))
    register_function("[*:]", (dtype("str"),dtype("int32"),), dtype("str"))
    register_function("get", (dtype("dict<?key, ?value>"),dtype("?key"),), dtype("?value"))
    register_function("get", (dtype("dict<?key, ?value>"),dtype("?key"),dtype("?value"),), dtype("?value"))
    register_function("max", (dtype("array<?T:numeric>"),), dtype("?T"))
    register_function("max", (dtype("set<?T:numeric>"),), dtype("?T"))
    register_function("max", (dtype("?T"),dtype("?T"),), dtype("?T"))
    register_function("product", (dtype("set<?T:numeric>"),), dtype("?T"))
    register_function("product", (dtype("array<?T:numeric>"),), dtype("?T"))
    register_function("toInt32", (dtype("?T:numeric"),), dtype("int32"))
    register_function("extend", (dtype("array<?T>"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("argmin", (dtype("array<?T>"),), dtype("int32"))
    register_function("toFloat64", (dtype("?T:numeric"),), dtype("float64"))
    register_function("sort", (dtype("array<?T>"),), dtype("array<?T>"))
    register_function("sort", (dtype("array<?T>"),dtype("bool"),), dtype("array<?T>"))
    register_function("isSubset", (dtype("set<?T>"),dtype("set<?T>"),), dtype("bool"))
    register_function("[*:*]", (dtype("str"),dtype("int32"),dtype("int32"),), dtype("str"))
    register_function("[*:*]", (dtype("array<?T>"),dtype("int32"),dtype("int32"),), dtype("array<?T>"))
    register_function("+", (dtype("array<?T:numeric>"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("+", (dtype("array<?T:numeric>"),dtype("?T"),), dtype("array<?T>"))
    register_function("+", (dtype("?T:numeric"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("**", (dtype("?T:numeric"),dtype("array<?T>"),), dtype("array<float64>"))
    register_function("**", (dtype("array<?T:numeric>"),dtype("array<?T>"),), dtype("array<float64>"))
    register_function("**", (dtype("array<?T:numeric>"),dtype("?T"),), dtype("array<float64>"))
    register_function("append", (dtype("array<?T>"),dtype("?T"),), dtype("array<?T>"))
    register_function("[:*]", (dtype("str"),dtype("int32"),), dtype("str"))
    register_function("[:*]", (dtype("array<?T>"),dtype("int32"),), dtype("array<?T>"))
    register_function("remove", (dtype("set<?T>"),dtype("?T"),), dtype("set<?T>"))
    register_function("[]", (dtype("str"),dtype("int32"),), dtype("str"))
    register_function("[]", (dtype("array<?T>"),dtype("int32"),), dtype("?T"))
    register_function("[]", (dtype("dict<?key, ?value>"),dtype("?key"),), dtype("?value"))
    register_function("dictToArray", (dtype("dict<?key, ?value>"),), dtype("array<tuple(?key, ?value)>"))
    register_function("%", (dtype("array<?T:numeric>"),dtype("?T"),), dtype("array<?T>"))
    register_function("%", (dtype("?T:numeric"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("%", (dtype("array<?T:numeric>"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("dict", (dtype("array<tuple(?key, ?value)>"),), dtype("dict<?key, ?value>"))
    register_function("dict", (dtype("set<tuple(?key, ?value)>"),), dtype("dict<?key, ?value>"))
    register_function("keys", (dtype("dict<?key, ?value>"),), dtype("array<?key>"))
    register_function("min", (dtype("array<?T:numeric>"),), dtype("?T"))
    register_function("min", (dtype("set<?T:numeric>"),), dtype("?T"))
    register_function("min", (dtype("?T"),dtype("?T"),), dtype("?T"))
    register_function("sum", (dtype("set<?T:numeric>"),), dtype("?T"))
    register_function("sum", (dtype("array<?T:numeric>"),), dtype("?T"))
    register_function("toInt64", (dtype("?T:numeric"),), dtype("int64"))
    register_function("contains", (dtype("dict<?key, ?value>"),dtype("?key"),), dtype("bool"))
    register_function("contains", (dtype("set<?T>"),dtype("?T"),), dtype("bool"))
    register_function("-", (dtype("?T:numeric"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("-", (dtype("array<?T:numeric>"),dtype("?T"),), dtype("array<?T>"))
    register_function("-", (dtype("array<?T:numeric>"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("addone", (dtype("int32"),), dtype("int32"))
    register_function("isEmpty", (dtype("dict<?key, ?value>"),), dtype("bool"))
    register_function("isEmpty", (dtype("array<?T>"),), dtype("bool"))
    register_function("isEmpty", (dtype("set<?T>"),), dtype("bool"))
    register_function("[:]", (dtype("array<?T>"),), dtype("array<?T>"))
    register_function("[:]", (dtype("str"),), dtype("str"))
    register_function("union", (dtype("set<?T>"),dtype("set<?T>"),), dtype("set<?T>"))
    register_function("*", (dtype("array<?T:numeric>"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("*", (dtype("?T:numeric"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("*", (dtype("array<?T:numeric>"),dtype("?T"),), dtype("array<?T>"))
    register_function("intersection", (dtype("set<?T>"),dtype("set<?T>"),), dtype("set<?T>"))
    register_function("add", (dtype("set<?T>"),dtype("?T"),), dtype("set<?T>"))
    register_function("argmax", (dtype("array<?T>"),), dtype("int32"))
    register_function("//", (dtype("array<?T:numeric>"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("//", (dtype("array<?T:numeric>"),dtype("?T"),), dtype("array<?T>"))
    register_function("//", (dtype("?T:numeric"),dtype("array<?T>"),), dtype("array<?T>"))
    register_function("keySet", (dtype("dict<?key, ?value>"),), dtype("set<?key>"))
    register_function("qnorm", (dtype("float64"),), dtype("float64"))
    register_function("oneHotAlleles", (dtype("call"),dtype("int32"),), dtype("array<int32>"))
    register_function("dpois", (dtype("float64"),dtype("float64"),dtype("bool"),), dtype("float64"))
    register_function("dpois", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("ploidy", (dtype("call"),), dtype("int32"))
    register_function("||", (dtype("bool"),dtype("bool"),), dtype("bool"))
    register_function("ppois", (dtype("float64"),dtype("float64"),dtype("bool"),dtype("bool"),), dtype("float64"))
    register_function("ppois", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("log10", (dtype("float64"),), dtype("float64"))
    register_function("isHet", (dtype("call"),), dtype("bool"))
    register_function("isAutosomalOrPseudoAutosomal", (dtype("?T:locus"),), dtype("bool"))
    register_function("testCodeUnification", (dtype("?x:numeric"),dtype("?x:int32"),), dtype("?x"))
    register_seeded_function("rand_pois", (dtype("float64"),), dtype("float64"))
    register_seeded_function("rand_pois", (dtype("int32"),dtype("float64"),), dtype("array<float64>"))
    register_function("toFloat32", (dtype("str"),), dtype("float32"))
    register_function("toFloat32", (dtype("bool"),), dtype("float32"))
    register_function("isAutosomal", (dtype("?T:locus"),), dtype("bool"))
    register_function("isPhased", (dtype("call"),), dtype("bool"))
    register_function("isHomVar", (dtype("call"),), dtype("bool"))
    register_function("corr", (dtype("array<float64>"),dtype("array<float64>"),), dtype("float64"))
    register_function("log", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("log", (dtype("float64"),), dtype("float64"))
    register_function("foobar2", (), dtype("int32"))
    register_function("approxEqual", (dtype("float64"),dtype("float64"),dtype("float64"),dtype("bool"),dtype("bool"),), dtype("bool"))
    register_function("plDosage", (dtype("array<?N:int32>"),), dtype("float64"))
    register_function("includesEnd", (dtype("interval<?T>"),), dtype("bool"))
    register_function("position", (dtype("?T:locus"),), dtype("int32"))
    register_seeded_function("rand_unif", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("str", (dtype("?T"),), dtype("str"))
    register_function("replace", (dtype("str"),dtype("str"),dtype("str"),), dtype("str"))
    register_function("exp", (dtype("float64"),), dtype("float64"))
    register_function("&&", (dtype("bool"),dtype("bool"),), dtype("bool"))
    register_function("compare", (dtype("int32"),dtype("int32"),), dtype("int32"))
    register_function("triangle", (dtype("int32"),), dtype("int32"))
    register_function("Interval", (dtype("?T"),dtype("?T"),dtype("bool"),dtype("bool"),), dtype("interval<?T>"))
    register_function("contig", (dtype("?T:locus"),), dtype("str"))
    register_function("Call", (dtype("bool"),), dtype("call"))
    register_function("Call", (dtype("str"),), dtype("call"))
    register_function("Call", (dtype("int32"),dtype("bool"),), dtype("call"))
    register_function("Call", (dtype("int32"),dtype("int32"),dtype("bool"),), dtype("call"))
    register_function("Call", (dtype("array<int32>"),dtype("bool"),), dtype("call"))
    register_function("qchisqtail", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("binomTest", (dtype("int32"),dtype("int32"),dtype("float64"),dtype("int32"),), dtype("float64"))
    register_function("qpois", (dtype("float64"),dtype("float64"),), dtype("int32"))
    register_function("qpois", (dtype("float64"),dtype("float64"),dtype("bool"),dtype("bool"),), dtype("int32"))
    register_function("is_finite", (dtype("float32"),), dtype("bool"))
    register_function("is_finite", (dtype("float64"),), dtype("bool"))
    register_function("inYPar", (dtype("?T:locus"),), dtype("bool"))
    register_function("contingency_table_test", (dtype("int32"),dtype("int32"),dtype("int32"),dtype("int32"),dtype("int32"),), dtype("struct{p_value: float64, odds_ratio: float64}"))
    register_function("toInt32", (dtype("bool"),), dtype("int32"))
    register_function("toInt32", (dtype("str"),), dtype("int32"))
    register_function("foobar1", (), dtype("int32"))
    register_function("toFloat64", (dtype("str"),), dtype("float64"))
    register_function("toFloat64", (dtype("bool"),), dtype("float64"))
    register_function("dbeta", (dtype("float64"),dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("min_rep", (dtype("?T:locus"),dtype("array<str>"),), dtype("struct{locus: ?T, alleles: array<str>}"))
    register_function("toBoolean", (dtype("str"),), dtype("bool"))
    register_seeded_function("rand_bool", (dtype("float64"),), dtype("bool"))
    register_function("pchisqtail", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_seeded_function("rand_cat", (dtype("array<float64>"),), dtype("int32"))
    register_function("inYNonPar", (dtype("?T:locus"),), dtype("bool"))
    register_function("+", (dtype("str"),dtype("str"),), dtype("str"))
    register_function("**", (dtype("float32"),dtype("float32"),), dtype("float64"))
    register_function("**", (dtype("int32"),dtype("int32"),), dtype("float64"))
    register_function("**", (dtype("int64"),dtype("int64"),), dtype("float64"))
    register_function("**", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("split", (dtype("str"),dtype("str"),dtype("int32"),), dtype("array<str>"))
    register_function("split", (dtype("str"),dtype("str"),), dtype("array<str>"))
    register_seeded_function("rand_gamma", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("UnphasedDiploidGtIndexCall", (dtype("int32"),), dtype("call"))
    register_function("[]", (dtype("call"),dtype("int32"),), dtype("int32"))
    register_function("sign", (dtype("int64"),), dtype("int64"))
    register_function("sign", (dtype("float64"),), dtype("float64"))
    register_function("sign", (dtype("float32"),), dtype("float32"))
    register_function("sign", (dtype("int32"),), dtype("int32"))
    register_function("unphasedDiploidGtIndex", (dtype("call"),), dtype("int32"))
    register_function("gamma", (dtype("float64"),), dtype("float64"))
    register_function("%", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("%", (dtype("int64"),dtype("int64"),), dtype("int64"))
    register_function("%", (dtype("float32"),dtype("float32"),), dtype("float32"))
    register_function("%", (dtype("int32"),dtype("int32"),), dtype("int32"))
    register_function("fisher_exact_test", (dtype("int32"),dtype("int32"),dtype("int32"),dtype("int32"),), dtype("struct{p_value: float64, odds_ratio: float64, ci_95_lower: float64, ci_95_upper: float64}"))
    register_function("floor", (dtype("float64"),), dtype("float64"))
    register_function("floor", (dtype("float32"),), dtype("float32"))
    register_function("isNonRef", (dtype("call"),), dtype("bool"))
    register_function("includesStart", (dtype("interval<?T>"),), dtype("bool"))
    register_function("isHetNonRef", (dtype("call"),), dtype("bool"))
    register_function("hardy_weinberg_test", (dtype("int32"),dtype("int32"),dtype("int32"),), dtype("struct{het_freq_hwe: float64, p_value: float64}"))
    register_function("haplotype_freq_em", (dtype("array<int32>"),), dtype("array<float64>"))
    register_function("nNonRefAlleles", (dtype("call"),), dtype("int32"))
    register_function("abs", (dtype("float64"),), dtype("float64"))
    register_function("abs", (dtype("float32"),), dtype("float32"))
    register_function("abs", (dtype("int64"),), dtype("int64"))
    register_function("abs", (dtype("int32"),), dtype("int32"))
    register_function("endswith", (dtype("str"),dtype("str"),), dtype("bool"))
    register_function("sqrt", (dtype("float64"),), dtype("float64"))
    register_function("isnan", (dtype("float32"),), dtype("bool"))
    register_function("isnan", (dtype("float64"),), dtype("bool"))
    register_function("lower", (dtype("str"),), dtype("str"))
    register_seeded_function("rand_beta", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_seeded_function("rand_beta", (dtype("float64"),dtype("float64"),dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("toInt64", (dtype("bool"),), dtype("int64"))
    register_function("toInt64", (dtype("str"),), dtype("int64"))
    register_function("testCodeUnification2", (dtype("?x"),), dtype("?x"))
    register_function("contains", (dtype("str"),dtype("str"),), dtype("bool"))
    register_function("contains", (dtype("interval<?T>"),dtype("?T"),), dtype("bool"))
    register_function("entropy", (dtype("str"),), dtype("float64"))
    register_function("filtering_allele_frequency", (dtype("int32"),dtype("int32"),dtype("float64"),), dtype("float64"))
    register_function("gqFromPL", (dtype("array<?N:int32>"),), dtype("int32"))
    register_function("startswith", (dtype("str"),dtype("str"),), dtype("bool"))
    register_function("ceil", (dtype("float32"),), dtype("float32"))
    register_function("ceil", (dtype("float64"),), dtype("float64"))
    register_function("json", (dtype("?T"),), dtype("str"))
    register_function("strip", (dtype("str"),), dtype("str"))
    register_function("firstMatchIn", (dtype("str"),dtype("str"),), dtype("array<str>"))
    register_function("isEmpty", (dtype("interval<?T>"),), dtype("bool"))
    register_function("~", (dtype("str"),dtype("str"),), dtype("bool"))
    register_function("mkString", (dtype("set<str>"),dtype("str"),), dtype("str"))
    register_function("mkString", (dtype("array<str>"),dtype("str"),), dtype("str"))
    register_function("dosage", (dtype("array<?N:float64>"),), dtype("float64"))
    register_function("upper", (dtype("str"),), dtype("str"))
    register_function("overlaps", (dtype("interval<?T>"),dtype("interval<?T>"),), dtype("bool"))
    register_function("downcode", (dtype("call"),dtype("int32"),), dtype("call"))
    register_function("inXPar", (dtype("?T:locus"),), dtype("bool"))
    register_function("format", (dtype("str"),dtype("?T:tuple"),), dtype("str"))
    register_function("pnorm", (dtype("float64"),), dtype("float64"))
    register_function("is_infinite", (dtype("float32"),), dtype("bool"))
    register_function("is_infinite", (dtype("float64"),), dtype("bool"))
    register_function("isHetRef", (dtype("call"),), dtype("bool"))
    register_function("isMitochondrial", (dtype("?T:locus"),), dtype("bool"))
    register_function("hamming", (dtype("str"),dtype("str"),), dtype("int32"))
    register_function("end", (dtype("interval<?T>"),), dtype("?T"))
    register_function("start", (dtype("interval<?T>"),), dtype("?T"))
    register_function("inXNonPar", (dtype("?T:locus"),), dtype("bool"))
    register_function("escapeString", (dtype("str"),), dtype("str"))
    register_function("isHomRef", (dtype("call"),), dtype("bool"))
    register_seeded_function("rand_norm", (dtype("float64"),dtype("float64"),), dtype("float64"))
    register_function("chi_squared_test", (dtype("int32"),dtype("int32"),dtype("int32"),dtype("int32"),), dtype("struct{p_value: float64, odds_ratio: float64}"))
