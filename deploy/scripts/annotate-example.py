import hail
hc = hail.HailContext()

input_path = 'gs://seqr-hail/test-data/Cohen.vep.vds'
output_path = 'gs://seqr-hail/annotated/Cohen.vds'

ref_dir = 'gs://seqr-hail/reference_data'

vds = (hc.read(input_path)
       .annotate_variants_vds(hc.read(ref_dir + '/clinvar/clinvar_v2016_09_01.vds'),
                              root='va.clinvar')
       .annotate_variants_vds(hc.read(ref_dir + '/exac/exac_v1.vds'),
                              root='va.exac')
       .annotate_variants_vds(hc.read(ref_dir + '/1kg/1kg_wgs_phase3.vds'),
                              root='va.g1k')
       .annotate_variants_vds(hc.read(ref_dir + '/dbnsfp/dbNSFP_3.2a_variant.filtered.allhg19_nodup.vds'),
                              root='va.dbnsfp')
       .annotate_variants_expr('''
va.vep.sorted_transcript_consequences = va.vep.transcript_consequences
  .map(c => select(c, amino_acids, biotype, canonical, cdna_start, cdna_end, codons, consequence_terms, distance, domains, exon, gene_id, transcript_id, protein_id, gene_symbol, gene_symbol_source, hgnc_id, hgvsc, hgvsp, lof, lof_flags, lof_filter, lof_info))
  .sortBy(c =>
        let is_coding = (c.biotype=="protein_coding") 
        and is_most_severe = c.consequence_terms.toSet.contains(va.vep.most_severe_consequence)
        and is_canonical = (c.canonical == 1) in
            if (is_coding)
                if(is_most_severe)
                    if (is_canonical)  1  else  2
                else  3
            else
                if (is_most_severe)
                    if (is_canonical)  4  else  5
                else  6
        )
''')
       .annotate_variants_expr('''
       va.tmp.filters = va.filters,
       va.tmp.pass = va.pass,
       va.tmp.rsid = va.rsid,
       va.tmp.was_split = va.wasSplit,
       va.tmp.info = va.info,
       va.tmp.clinvar_clinsig = va.clinvar.clinical_significance,
       va.tmp.clinvar_review_status = va.clinvar.review_status,
       va.tmp.clinvar_inheritance_mode = va.clinvar.inheritance_modes.split(";").toSet,
       va.tmp.clinvar_disease_mechanism = va.clinvar.disease_mechanism.split(";").toSet,
       va.tmp.clinvar_gold_stars = va.clinvar.gold_stars,
       va.tmp.clinvar_is_pathogenic = va.clinvar.pathogenic,
       va.tmp.clinvar_is_conflicted = va.clinvar.conflicted,
       va.tmp.vep_gene_id = va.vep.transcript_consequences.map(x => x.gene_id).toSet,
       va.tmp.vep_transcript_id = va.vep.transcript_consequences.map(x => x.transcript_id).toSet,
       va.tmp.vep_most_severe_consequence = va.vep.most_severe_consequence,
       va.tmp.g1k_wgs_phase3_popmax_af = va.g1k.info.POPMAX_AF,
       va.tmp.g1k_wgs_phase3_popmax = va.g1k.info.POPMAX,
       va.tmp.exac_v1_popmax = va.exac.info.POPMAX[va.exac.aIndex-1],
       va.tmp.twinsuk_af = va.dbnsfp.TWINSUK_AF.toDouble,
       va.tmp.alspac_af = va.dbnsfp.ALSPAC_AF.toDouble,
       va.tmp.esp65000_aa_af = va.dbnsfp.ESP6500_AA_AF.toDouble,
       va.tmp.esp65000_ea_af = va.dbnsfp.ESP6500_EA_AF.toDouble,
       va.tmp.dbnsfp_sift_pred = va.dbnsfp.SIFT_pred,
       va.tmp.dbnsfp_polyphen2_hdiv_pred = va.dbnsfp.Polyphen2_HDIV_pred,
       va.tmp.dbnsfp_polyphen2_hvar_pred = va.dbnsfp.Polyphen2_HVAR_pred,
       va.tmp.dbnsfp_lrt_pred = va.dbnsfp.LRT_pred,
       va.tmp.dbnsfp_muttaster_pred = va.dbnsfp.MutationTaster_pred,
       va.tmp.dbnsfp_mutassesor_pred = va.dbnsfp.MutationAssessor_pred,
       va.tmp.dbnsfp_fathmm_pred = va.dbnsfp.FATHMM_pred,
       va.tmp.dbnsfp_provean_pred = va.dbnsfp.PROVEAN_pred,
       va.tmp.dbnsfp_metasvm_pred = va.dbnsfp.MetaSVM_pred,
       va.tmp.dbnsfp_metalr_pred = va.dbnsfp.MetaLR_pred,
       va.tmp.dbnsfp_cadd_phred = va.dbnsfp.CADD_phred,
       va.tmp.vep_consequences = va.vep.transcript_consequences.map(x => x.consequence_terms).flatten().toSet,
       va.tmp.clinvar_submitter = va.clinvar.all_submitters.split(";").toSet,
       va.tmp.clinvar_trait = va.clinvar.all_traits.split(";").toSet,
       va.tmp.clinvar_pmid = va.clinvar.all_pmids.split(";").toSet,
       va.tmp.clinvar_age_of_onset = va.clinvar.age_of_onset.split(";").toSet,
       va.tmp.clinvar_prevalence = va.clinvar.prevalence.split(";").toSet,
       va.tmp.clinvar_origin = va.clinvar.origin.split(";").toSet,
       va.tmp.clinvar_xrefs = va.clinvar.xrefs
''')
       .annotate_variants_expr('va = va.tmp'))

print(vds.variant_schema)

vds.write(output_path)
