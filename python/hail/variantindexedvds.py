from pyhail.java import jarray
from pyhail.dataset import VariantDataset

class VariantIndexedVDS(object):
    def __init__(self, hc, path):
        self.hc = hc
        self.jvds = self.hc.jvm.org.broadinstitute.hail.variant.VariantIndexedVDS.apply(hc.jsql_context, path)

    def query_from_file(self, path):
        return VariantDataset(self.hc, self.jvds.query(path))

    def query_from_variant_list(self, variants):
        return VariantDataset(
            self.hc,
            self.jvds.query(
                jarray(self.hc.gateway, self.hc.jvm.org.broadinstitute.hail.variant.Variant,
                       [v.jv for v in variants])))
