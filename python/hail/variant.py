from pyhail.java import jarray

class Variant(object):
    def __init__(self, hc, contig, start, ref, alts):
        if (isinstance(alts, str)):
            alts = [alts]

        self.hc = hc
        self.jv = self.hc.jvm.org.broadinstitute.hail.variant.Variant.apply(
            contig, start, ref,
            jarray(self.hc.gateway, self.hc.jvm.java.lang.String, alts))
