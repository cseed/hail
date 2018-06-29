"""
Unit tests for Hail.
"""
import pandas as pd
import unittest
import random
import math

import hail as hl
import hail.expr.aggregators as agg
from hail.utils.java import Env, scala_object
from hail.utils.misc import new_temp_file
import pyspark.sql
from test.utils import resource, startTestHailContext, stopTestHailContext, convert_struct_to_dict
import operator

setUpModule = startTestHailContext
tearDownModule = stopTestHailContext


class TestFunctions(unittest.TestCase):
    def test(self):
        schema = hl.tstruct(a=hl.tint32, b=hl.tint32, c=hl.tint32, d=hl.tint32, e=hl.tstr,
                            f=hl.tarray(hl.tint32),
                            g=hl.tarray(
                                hl.tstruct(x=hl.tint32, y=hl.tint32, z=hl.tstr)),
                            h=hl.tstruct(a=hl.tint32, b=hl.tint32, c=hl.tstr),
                            i=hl.tbool,
                            j=hl.tstruct(x=hl.tint32, y=hl.tint32, z=hl.tstr))

        rows = [{'a': 4, 'b': 1, 'c': 3, 'd': 5,
                 'e': "hello", 'f': [1, 2, 3],
                 'g': [hl.Struct(x=1, y=5, z='banana')],
                 'h': hl.Struct(a=5, b=3, c='winter'),
                 'i': True,
                 'j': hl.Struct(x=3, y=2, z='summer')}]

        kt = hl.Table.parallelize(rows, schema)

        result = convert_struct_to_dict(kt.annotate(
            chisq=hl.chisq(kt.a, kt.b, kt.c, kt.d),
            ctt=hl.ctt(kt.a, kt.b, kt.c, kt.d, 5),
            dict=hl.dict(hl.zip([kt.a, kt.b], [kt.c, kt.d])),
            dpois=hl.dpois(4, kt.a),
            drop=kt.h.drop('b', 'c'),
            exp=hl.exp(kt.c),
            fet=hl.fisher_exact_test(kt.a, kt.b, kt.c, kt.d),
            hwe=hl.hardy_weinberg_p(1, 2, 1),
            is_defined=hl.is_defined(kt.i),
            is_missing=hl.is_missing(kt.i),
            is_nan=hl.is_nan(hl.float64(kt.a)),
            json=hl.json(kt.g),
            log=hl.log(kt.a, kt.b),
            log10=hl.log10(kt.c),
            or_else=hl.or_else(kt.a, 5),
            or_missing=hl.or_missing(kt.i, kt.j),
            pchisqtail=hl.pchisqtail(kt.a, kt.b),
            pcoin=hl.rand_bool(0.5),
            pnorm=hl.pnorm(0.2),
            pow=2.0 ** kt.b,
            ppois=hl.ppois(kt.a, kt.b),
            qchisqtail=hl.qchisqtail(kt.a, kt.b),
            range=hl.range(0, 5, kt.b),
            rnorm=hl.rand_norm(0.0, kt.b),
            rpois=hl.rand_pois(kt.a),
            runif=hl.rand_unif(kt.b, kt.a),
            select=kt.h.select('c', 'b'),
            sqrt=hl.sqrt(kt.a),
            to_str=[hl.str(5), hl.str(kt.a), hl.str(kt.g)],
            where=hl.cond(kt.i, 5, 10)
        ).take(1)[0])

        # print(result) # Fixme: Add asserts


class TestColumn(unittest.TestCase):
    def test_operators(self):
        schema = hl.tstruct(a=hl.tint32, b=hl.tint32, c=hl.tint32, d=hl.tint32, e=hl.tstr, f=hl.tarray(hl.tint32))

        rows = [{'a': 4, 'b': 1, 'c': 3, 'd': 5, 'e': "hello", 'f': [1, 2, 3]},
                {'a': 0, 'b': 5, 'c': 13, 'd': -1, 'e': "cat", 'f': []},
                {'a': 4, 'b': 2, 'c': 20, 'd': 3, 'e': "dog", 'f': [5, 6, 7]}]

        kt = hl.Table.parallelize(rows, schema)

        result = convert_struct_to_dict(kt.annotate(
            x1=kt.a + 5,
            x2=5 + kt.a,
            x3=kt.a + kt.b,
            x4=kt.a - 5,
            x5=5 - kt.a,
            x6=kt.a - kt.b,
            x7=kt.a * 5,
            x8=5 * kt.a,
            x9=kt.a * kt.b,
            x10=kt.a / 5,
            x11=5 / kt.a,
            x12=kt.a / kt.b,
            x13=-kt.a,
            x14=+kt.a,
            x15=kt.a == kt.b,
            x16=kt.a == 5,
            x17=5 == kt.a,
            x18=kt.a != kt.b,
            x19=kt.a != 5,
            x20=5 != kt.a,
            x21=kt.a > kt.b,
            x22=kt.a > 5,
            x23=5 > kt.a,
            x24=kt.a >= kt.b,
            x25=kt.a >= 5,
            x26=5 >= kt.a,
            x27=kt.a < kt.b,
            x28=kt.a < 5,
            x29=5 < kt.a,
            x30=kt.a <= kt.b,
            x31=kt.a <= 5,
            x32=5 <= kt.a,
            x33=(kt.a == 0) & (kt.b == 5),
            x34=(kt.a == 0) | (kt.b == 5),
            x35=False,
            x36=True
        ).take(1)[0])

        expected = {'a': 4, 'b': 1, 'c': 3, 'd': 5, 'e': "hello", 'f': [1, 2, 3],
                    'x1': 9, 'x2': 9, 'x3': 5,
                    'x4': -1, 'x5': 1, 'x6': 3,
                    'x7': 20, 'x8': 20, 'x9': 4,
                    'x10': 4.0 / 5, 'x11': 5.0 / 4, 'x12': 4, 'x13': -4, 'x14': 4,
                    'x15': False, 'x16': False, 'x17': False,
                    'x18': True, 'x19': True, 'x20': True,
                    'x21': True, 'x22': False, 'x23': True,
                    'x24': True, 'x25': False, 'x26': True,
                    'x27': False, 'x28': True, 'x29': False,
                    'x30': False, 'x31': True, 'x32': False,
                    'x33': False, 'x34': False, 'x35': False, 'x36': True}

        for k, v in expected.items():
            if isinstance(v, float):
                self.assertAlmostEqual(v, result[k], msg=k)
            else:
                self.assertEqual(v, result[k], msg=k)

    def test_array_column(self):
        schema = hl.tstruct(a=hl.tarray(hl.tint32))
        rows = [{'a': [1, 2, 3]}]
        kt = hl.Table.parallelize(rows, schema)

        result = convert_struct_to_dict(kt.annotate(
            x1=kt.a[0],
            x2=kt.a[2],
            x3=kt.a[:],
            x4=kt.a[1:2],
            x5=kt.a[-1:2],
            x6=kt.a[:2]
        ).take(1)[0])

        expected = {'a': [1, 2, 3], 'x1': 1, 'x2': 3, 'x3': [1, 2, 3],
                    'x4': [2], 'x5': [], 'x6': [1, 2]}

        self.assertDictEqual(result, expected)

    def test_dict_column(self):
        schema = hl.tstruct(x=hl.tfloat64)
        rows = [{'x': 2.0}]
        kt = hl.Table.parallelize(rows, schema)

        kt = kt.annotate(a={'cat': 3, 'dog': 7})

        result = convert_struct_to_dict(kt.annotate(
            x1=kt.a['cat'],
            x2=kt.a['dog'],
            x3=kt.a.keys().contains('rabbit'),
            x4=kt.a.size() == 0,
            x5=kt.a.key_set(),
            x6=kt.a.keys(),
            x7=kt.a.values(),
            x8=kt.a.size(),
            x9=kt.a.map_values(lambda v: v * 2.0)
        ).take(1)[0])

        expected = {'a': {'cat': 3, 'dog': 7}, 'x': 2.0, 'x1': 3, 'x2': 7, 'x3': False,
                    'x4': False, 'x5': {'cat', 'dog'}, 'x6': ['cat', 'dog'],
                    'x7': [3, 7], 'x8': 2, 'x9': {'cat': 6.0, 'dog': 14.0}}

        self.assertDictEqual(result, expected)

    def test_numeric_conversion(self):
        schema = hl.tstruct(a=hl.tfloat64, b=hl.tfloat64, c=hl.tint32, d=hl.tint32)
        rows = [{'a': 2.0, 'b': 4.0, 'c': 1, 'd': 5}]
        kt = hl.Table.parallelize(rows, schema)
        kt = kt.annotate(d=hl.int64(kt.d))

        kt = kt.annotate(x1=[1.0, kt.a, 1],
                         x2=[1, 1.0],
                         x3=[kt.a, kt.c],
                         x4=[kt.c, kt.d],
                         x5=[1, kt.c])

        expected_schema = {'a': hl.tfloat64,
                           'b': hl.tfloat64,
                           'c': hl.tint32,
                           'd': hl.tint64,
                           'x1': hl.tarray(hl.tfloat64),
                           'x2': hl.tarray(hl.tfloat64),
                           'x3': hl.tarray(hl.tfloat64),
                           'x4': hl.tarray(hl.tint64),
                           'x5': hl.tarray(hl.tint32)}

        for f, t in kt.row.dtype.items():
            self.assertEqual(expected_schema[f], t)

    def test_constructors(self):
        rg = hl.ReferenceGenome("foo", ["1"], {"1": 100})

        schema = hl.tstruct(a=hl.tfloat64, b=hl.tfloat64, c=hl.tint32, d=hl.tint32)
        rows = [{'a': 2.0, 'b': 4.0, 'c': 1, 'd': 5}]
        kt = hl.Table.parallelize(rows, schema)
        kt = kt.annotate(d=hl.int64(kt.d))

        kt = kt.annotate(l1=hl.parse_locus("1:51"),
                         l2=hl.locus("1", 51, reference_genome=rg),
                         i1=hl.parse_locus_interval("1:51-56", reference_genome=rg),
                         i2=hl.interval(hl.locus("1", 51, reference_genome=rg),
                                        hl.locus("1", 56, reference_genome=rg)))

        expected_schema = {'a': hl.tfloat64, 'b': hl.tfloat64, 'c': hl.tint32, 'd': hl.tint64,
                           'l1': hl.tlocus(), 'l2': hl.tlocus(rg),
                           'i1': hl.tinterval(hl.tlocus(rg)), 'i2': hl.tinterval(hl.tlocus(rg))}

        self.assertTrue(all([expected_schema[f] == t for f, t in kt.row.dtype.items()]))
