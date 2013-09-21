'''
Created on May 6, 2013

@author: kpaskov
'''
from convert_aux.auxillary_tables import update_biocon_gene_counts, \
    convert_biocon_ancestors, convert_biofact
from schema_conversion import create_or_update_and_remove, \
    prepare_schema_connection, cache_by_key, cache_by_id, create_format_name, \
    create_or_update, new_config, execute_conversion, old_config
from schema_conversion.output_manager import write_to_output_file
from sqlalchemy.orm import joinedload
from utils.link_maker import biocon_link, allele_link, chemical_link
import model_new_schema
import model_old_schema

'''
 This code is used to convert phenotype data from the old schema to the new. It does this by
 creating new schema objects from the old, then comparing these new objects to those already
 stored in the new database. If a newly created object matches one that is already stored, the two
 are compared and the database fields are updated. If a newly created object does not match one that is 
 already stored, it is added to the database.
'''
"""
------------ Create --------------
"""
def create_phenotype_id(old_phenotype_id):
    return old_phenotype_id + 60000000

def create_phenoevidence_id(old_evidence_id):
    return old_evidence_id + 60000000

def create_phenotype_display_name(observable, qualifier, mutant_type):
    if mutant_type is None:
        mutant_type = 'None'
    if qualifier is None:
        display_name = observable + ' in ' + mutant_type + ' mutant'
    else:
        display_name = qualifier + ' ' + observable + ' in ' + mutant_type + ' mutant'
    return display_name
def create_phenotype_key(observable, qualifier, mutant_type):
    display_name = create_phenotype_display_name(observable, qualifier, mutant_type)
    format_name = create_format_name(display_name)
    return (format_name, 'PHENOTYPE')

def create_phenotype_type(observable):
    if observable in {'chemical compound accumulation', 'resistance to chemicals', 'osmotic stress resistance', 'alkaline pH resistance',
                      'ionic stress resistance', 'oxidative stress resistance', 'small molecule transport', 'metal resistance', 
                      'acid pH resistance', 'hyperosmotic stress resistance', 'hypoosmotic stress resistance', 'chemical compound excretion'}:
        return 'chemical'
    elif observable in {'protein/peptide accumulation', 'protein/peptide modification', 'protein/peptide distribution', 
                        'RNA accumulation', 'RNA localization', 'RNA modification'}:
        return 'pp_rna'
    else:
        return 'cellular'

def create_phenotype(old_phenotype):
    from model_new_schema.phenotype import Phenotype as NewPhenotype
    observable = old_phenotype.observable
    qualifier = old_phenotype.qualifier
    mutant_type = old_phenotype.mutant_type
    
    display_name = create_phenotype_display_name(observable, qualifier, mutant_type)
    format_name = create_format_name(display_name)
    link = biocon_link("Phenotype", format_name)
    new_phenotype = NewPhenotype(create_phenotype_id(old_phenotype.id), display_name, format_name, link,
                                 observable, qualifier, mutant_type, 
                                 create_phenotype_type(old_phenotype.observable),
                                 old_phenotype.date_created, old_phenotype.created_by)
    return new_phenotype

def create_aliases(old_cv_term, key_to_phenotype):
    from model_new_schema.bioconcept import Bioconceptalias as NewBioconceptalias
    
    phenotype_key = create_phenotype_key(old_cv_term.name)
    if phenotype_key not in key_to_phenotype:
        print 'Phenotype does not exist. ' + str(phenotype_key)
        return []
    biocon_id = key_to_phenotype[phenotype_key].id
    
    new_aliases = []
    for synonym in old_cv_term.synonyms:
        new_aliases.append(NewBioconceptalias(synonym, biocon_id, 'PHENOTYPE', 
                                   old_cv_term.date_created, old_cv_term.created_by))
    return new_aliases

def create_allele(old_phenotype_feature):
    from model_new_schema.misc import Allele as NewAllele
    if old_phenotype_feature.experiment is not None:
        allele_info = old_phenotype_feature.experiment.allele
        if allele_info is not None:
            link = allele_link(allele_info[0])
            new_allele = NewAllele(allele_info[0], allele_info[0], link, None)
            return new_allele
    return None

def create_phenoevidence(old_phenotype_feature, key_to_reflink, key_to_phenotype, 
                         id_to_reference, id_to_bioent, key_to_strain, key_to_experiment, key_to_allele):
    from model_new_schema.phenotype import Phenotypeevidence as NewPhenotypeevidence
    evidence_id = create_phenoevidence_id(old_phenotype_feature.id)
    reference_id = key_to_reflink[('PHENO_ANNOTATION_NO', old_phenotype_feature.id)].reference_id
    if reference_id not in id_to_reference:
        print 'Reference does not exist. ' + str(reference_id)
        return None
    
    bioent_id = old_phenotype_feature.feature_id
    if bioent_id not in id_to_bioent:
        print 'Bioentity does not exist. ' + str(bioent_id)
        return None
    
    phenotype_key = create_phenotype_key(old_phenotype_feature.observable, old_phenotype_feature.qualifier, old_phenotype_feature.mutant_type)
    if phenotype_key not in key_to_phenotype:
        print 'Phenotype does not exist. ' + str(phenotype_key)
        return None
    biocon_id = key_to_phenotype[phenotype_key].id
    
    experiment_key = create_format_name(old_phenotype_feature.experiment_type)
    if experiment_key not in key_to_experiment:
        print 'Experiment does not exist. ' + str(experiment_key)
        return None
    experiment_id = key_to_experiment[experiment_key].id

    strain_id = None
    mutant_allele_id = None
    allele_info = None
    reporter = None 
    reporter_desc = None
    strain_details = None
    experiment_details = None
    conditions = None
    details = None
                                        
    if old_phenotype_feature.experiment is not None:
        experiment = old_phenotype_feature.experiment
        reporter = None if experiment.reporter == None else experiment.reporter[0]
        reporter_desc = None if experiment.reporter == None else experiment.reporter[1]
        strain_key = None if experiment.strain == None else experiment.strain[0]
        strain_details = None if experiment.strain == None else experiment.strain[1]
        #new_phenoevidence.budding_index = None if experiment.budding_index == None else float(experiment.budding_index)
        #new_phenoevidence.glutathione_excretion = None if experiment.glutathione_excretion == None else float(experiment.glutathione_excretion)
        #new_phenoevidence.z_score = experiment.z_score
        #new_phenoevidence.relative_fitness_score = None if experiment.relative_fitness_score == None else float(experiment.relative_fitness_score)
        #new_phenoevidence.chitin_level = None if experiment.chitin_level == None else float(experiment.chitin_level)
    
        strain_id = None
        if strain_key in key_to_strain:
            strain_id = key_to_strain[strain_key].id
        
        allele_info = experiment.allele
        if allele_info is not None:
            allele_name = allele_info[0]
            mutant_allele_id = key_to_allele[allele_name].id
            allele_info = allele_info[1]
    
        comment = experiment.experiment_comment
        if comment is not None:
            experiment_details = comment
            
        if len(experiment.condition) > 0:
            conditions = []
            for (a, b) in experiment.condition:
                if b is None:
                    conditions.append(a)
                else:
                    conditions.append(a + '- ' + b)
            condition_info = ', '.join(conditions)
            conditions = condition_info
            
        if len(experiment.details) > 0:
            details = []
            for (a, b) in experiment.details:
                if b is None:
                    details.append(a)
                else:
                    details.append(a + '- ' + b)
            detail_info = ', '.join(details)
            details = detail_info
        
    new_phenoevidence = NewPhenotypeevidence(evidence_id, experiment_id, reference_id, strain_id,
                                         old_phenotype_feature.source,
                                         bioent_id, biocon_id,
                                         mutant_allele_id, allele_info, 
                                         reporter, reporter_desc, strain_details, experiment_details, conditions, details,
                                         old_phenotype_feature.date_created, old_phenotype_feature.created_by)
    return new_phenoevidence  

def create_chemicals(expt_property):
    from model_new_schema.chemical import Chemical as NewChemical

    display_name = expt_property.value
    format_name = create_format_name(display_name)
    link = chemical_link(format_name)
    new_chemical = NewChemical(display_name, format_name, link, 'SGD', expt_property.date_created, expt_property.created_by)
    return new_chemical

def create_evidence_chemical(chemical_info, evidence_id, key_to_chemical, id_to_phenoevidence):
    from model_new_schema.evidence import EvidenceChemical as NewEvidenceChemical
    chemical_key = create_format_name(chemical_info[0])
    if chemical_key not in key_to_chemical:
        print 'Chemical does not exist. ' + chemical_key
        return None
    chemical_id = key_to_chemical[chemical_key].id
    chemical_amount = chemical_info[1]
    
    if evidence_id not in id_to_phenoevidence:
        print 'Phenoevidence does not exist. ' + str(evidence_id)
        return None
    
    new_pheno_chemical = NewEvidenceChemical(evidence_id, chemical_id, chemical_amount)
    return new_pheno_chemical
  
"""
---------------------Convert------------------------------
"""  

def convert(old_session_maker, new_session_maker, ask):

    from model_new_schema.phenotype import Phenotypeevidence as NewPhenotypeevidence, Phenotype as NewPhenotype
    from model_old_schema.phenotype import PhenotypeFeature as OldPhenotypeFeature, Phenotype as OldPhenotype, ExperimentProperty as OldExperimentProperty
    from model_new_schema.bioconcept import BioconceptRelation as NewBioconceptRelation
    from model_old_schema.reference import Reflink as OldReflink
    from model_old_schema.cv import CVTerm as OldCVTerm
    
    # Convert phenotypes
    write_to_output_file( 'Phenotypes')
    execute_conversion(convert_phenotypes, old_session_maker, new_session_maker, ask,
                       old_phenotypes=lambda old_session: old_session.query(OldPhenotype).all(),
                       old_cv_terms=lambda old_session: old_session.query(OldCVTerm).filter(OldCVTerm.cv_no==6).options(
                                                    joinedload('cv_synonyms')).all())

#    # Convert aliases
#    print 'Aliases'
#    start_time = datetime.datetime.now()
#    try:
#        old_session = old_session_maker()
#        success=False
#        while not success:
#            new_session = new_session_maker()
#            success = convert_aliases(new_session, old_cv_terms)
#            ask_to_commit(new_session, start_time)  
#            new_session.close()
#    finally:
#        old_session.close()
#        new_session.close()
    
    # Convert alleles
    write_to_output_file( 'Alleles')
    execute_conversion(convert_alleles, old_session_maker, new_session_maker, ask,
                       old_phenoevidences=lambda old_session: old_session.query(OldPhenotypeFeature).all())
        
    # Convert phenoevidences
    write_to_output_file( 'Phenoevidences')
    execute_conversion(convert_phenoevidences, old_session_maker, new_session_maker, ask,
                       old_phenoevidences=lambda old_session: old_session.query(OldPhenotypeFeature).all(),
                       old_reflinks=lambda old_session: old_session.query(OldReflink).all())
      
    # Convert chemicals
    write_to_output_file( 'Chemicals')
    execute_conversion(convert_chemicals, old_session_maker, new_session_maker, ask,
                       old_expt_properties=lambda old_session: old_session.query(OldExperimentProperty).filter(OldExperimentProperty.type=='Chemical_pending').all())
          
    # Convert phenoevidence_chemicals
    write_to_output_file( 'Evidence_Chemicals')
    execute_conversion(convert_phenoevidence_chemicals, old_session_maker, new_session_maker, ask,
                       old_phenoevidences=lambda old_session: old_session.query(OldPhenotypeFeature).all())
    
    # Update gene counts
    write_to_output_file( 'Phenotype gene counts')
    execute_conversion(update_biocon_gene_counts, old_session_maker, new_session_maker, ask,
                       biocon_cls=lambda old_session:NewPhenotype,
                       evidence_cls=lambda old_session:NewPhenotypeevidence) 
##        
##    # Convert biocon_relations
##    write_to_output_file( 'Biocon_relations')  
##    execute_conversion(convert_biocon_relations, old_session_maker, new_session_maker, ask,
##                       old_cv_terms=lambda old_session: old_session.query(OldCVTerm).filter(OldCVTerm.cv_no==6).all()) 
##        
    intervals = [0, 500, 1000, 1500, 2000, 2500, 3000, 3500]
    new_session = new_session_maker()
    key_to_evidence = cache_by_key(NewPhenotypeevidence, new_session, evidence_type='PHENOTYPE')
    key_to_bioconrels = cache_by_key(NewBioconceptRelation, new_session, bioconrel_type='PHENOTYPE')
    new_session.close()
    
    # Convert biofacts
    write_to_output_file('Phenotype biofacts')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Biocon ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_biofact, old_session_maker, new_session_maker, ask,
                       key_to_evidence = lambda old_session: key_to_evidence,
                       key_to_bioconrels = lambda old_session: key_to_bioconrels,
                       biocon_type = lambda old_session: 'PHENOTYPE',
                       min_id=lambda old_session: min_id,
                       max_id=lambda old_session: max_id)  
        
    # Convert biocon_ancestors'
    write_to_output_file( 'Biocon_ancestors' )
    execute_conversion(convert_biocon_ancestors, old_session_maker, new_session_maker, ask,
                       bioconrel_type=lambda old_session:'PHENOTYPE',
                       num_generations=lambda old_session:4)   
        
    intervals = [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]

    new_session = new_session_maker()
    ask = False
    pheno_evidences = cache_by_key(NewPhenotypeevidence, new_session).values()     
    # Create bioent_reference
    write_to_output_file( 'Phenotype Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: pheno_evidences,
                       bioent_ref_type = lambda old_session: 'PHENOTYPE',
                       bioent_f = lambda old_session: lambda x: [x.bioent_id])
        
def convert_phenotypes(new_session, old_phenotypes, old_cv_terms):
    '''
    Convert Phenotypes
    '''
    from model_new_schema.phenotype import Phenotype as NewPhenotype
    
    #Cache phenotypes
    key_to_phenotype = cache_by_key(NewPhenotype, new_session)

    #Create new phenotypes if they don't exist, or update the database if they do.
    new_phenotypes = filter(None, [create_phenotype(x) for x in old_phenotypes])
    #new_key_to_phenotypes = dict([(x.unique_key(), x) for x in new_phenotypes])
    values_to_check = ['phenotype_type', 'display_name', 'description']
    
    #Add definitions to phenotypes
    #for cv_term in cv_terms:
    #    phenotype_key = create_phenotype_key(cv_term.name)
    #    if phenotype_key in new_key_to_phenotypes:
    #        new_phenotype = new_key_to_phenotypes[phenotype_key]
    #        new_phenotype.description = cv_term.definition
            
    success = create_or_update_and_remove(new_phenotypes, key_to_phenotype, values_to_check, new_session)
    return success

def convert_aliases(new_session, cv_terms):
    '''
    Convert Phenotypes
    '''
    from model_new_schema.bioconcept import Bioconceptalias as NewBioconceptalias
    from model_new_schema.phenotype import Phenotype as NewPhenotype
   
    #Cache aliases
    key_to_alias = cache_by_key(NewBioconceptalias, new_session, biocon_type='PHENOTYPE')
    key_to_phenotype = cache_by_key(NewPhenotype, new_session)

    #Create new aliases if they don't exist, or update the database if they do.
    new_aliases = []
    for cv_term in cv_terms:
        new_aliases.extend(create_aliases(cv_term, key_to_phenotype))

    values_to_check = ['source', 'used_for_search', 'date_created', 'created_by']  
    success = create_or_update_and_remove(new_aliases, key_to_alias, values_to_check, new_session)
    return success
                
def convert_alleles(new_session, old_phenoevidences):
    '''
    Convert Alleles
    '''
    from model_new_schema.misc import Allele as NewAllele
    #May be necessary so that alleles can be removed.

    #Cache alleles
    key_to_allele = cache_by_key(NewAllele, new_session)
    
    #Create new alleles if they don't exist, or update the database if they do.
    values_to_check = ['description', 'display_name']
    new_alleles = filter(None, [create_allele(x) for x in old_phenoevidences])
    success = create_or_update_and_remove(new_alleles, key_to_allele, values_to_check, new_session)
    return success

def convert_chemicals(new_session, old_expt_properties):
    '''
    Convert Chemical
    '''
    from model_new_schema.chemical import Chemical as NewChemical
    #Cache chemicals
    key_to_chemical = cache_by_key(NewChemical, new_session)
    
    #Create new chemicals if they don't exist, or update the database if they do.
    values_to_check = []
    new_chemicals = [create_chemicals(x) for x in old_expt_properties]
    success = create_or_update(new_chemicals, key_to_chemical, values_to_check, new_session)
    return success
    
def convert_phenoevidences(new_session, old_phenoevidences, old_reflinks):
    '''
    Convert Phenoevidences
    '''
    from model_new_schema.phenotype import Phenotypeevidence as NewPhenotypeevidence, Phenotype as NewPhenotype
    from model_new_schema.reference import Reference as NewReference
    from model_new_schema.bioentity import Bioentity as NewBioentity
    from model_new_schema.misc import Allele as NewAllele
    from model_new_schema.evelement import Strain as NewStrain, Experiment as NewExperiment
    
    #Cache reflinks
    key_to_reflink = dict([((x.col_name, x.primary_key), x) for x in old_reflinks])
    
    #Cache phenotypes, alleles, and phenoevidences
    key_to_phenotype = cache_by_key(NewPhenotype, new_session)
    key_to_allele = cache_by_key(NewAllele, new_session)
    key_to_phenoevidence = cache_by_key(NewPhenotypeevidence, new_session)
    id_to_reference = cache_by_id(NewReference, new_session)
    id_to_bioent = cache_by_id(NewBioentity, new_session)
    key_to_strain = cache_by_key(NewStrain, new_session)
    key_to_experiment = cache_by_key(NewExperiment, new_session)

    #Create new phenoevidences if they don't exist, or update the database if they do.    
    new_phenoevidences = [create_phenoevidence(old_phenoevidence, key_to_reflink, key_to_phenotype, id_to_reference, id_to_bioent, key_to_strain, key_to_experiment, key_to_allele)
                            for old_phenoevidence in old_phenoevidences]
   
    values_to_check = ['experiment_id', 'reference_id', 'strain_id', 'source',
                       'bioentity_id', 'bioconcept_id', 'date_created', 'created_by',
                       'reporter', 'reporter_desc', 'strain_details', 
                       'conditions', 'details', 'experiment_details', 'allele_info', 'allele_id']
    success = create_or_update_and_remove(new_phenoevidences, key_to_phenoevidence, values_to_check, new_session)
    return success
    
def convert_phenoevidence_chemicals(new_session, old_phenoevidences):
    '''
    Convert Phenoevidence_chemicals
    '''
    from model_new_schema.evidence import EvidenceChemical as NewEvidenceChemical
    from model_new_schema.phenotype import Phenotypeevidence as NewPhenotypeevidence
    from model_new_schema.chemical import Chemical as NewChemical
    
    #Cache evidence_chemical and chemical
    key_to_phenoevidence_chemical = cache_by_key(NewEvidenceChemical, new_session)
    key_to_chemical = cache_by_key(NewChemical, new_session)
    id_to_phenoevidence = cache_by_id(NewPhenotypeevidence, new_session)
    
    values_to_check = ['evidence_id', 'chemical_id', 'chemical_amt']
    phenoevidence_chemicals = []
    #Create new evidence_chemical if they don't exist, or update the database if they do.
    for old_phenoevidence in old_phenoevidences:
        new_phenoevidence_id = create_phenoevidence_id(old_phenoevidence.id)
        if old_phenoevidence.experiment is not None:
            chemical_infos = old_phenoevidence.experiment.chemicals
            if chemical_infos is not None:   
                phenoevidence_chemicals.extend([create_evidence_chemical(x, new_phenoevidence_id, key_to_chemical, id_to_phenoevidence) for x in chemical_infos])
    success = create_or_update_and_remove(phenoevidence_chemicals, key_to_phenoevidence_chemical, values_to_check, new_session)
    return success

def convert_biocon_relations(new_session, old_cv_terms):
    '''
    Convert biocon_relations (add phenotype ontology)
    '''         
    from model_new_schema.bioconcept import BioconceptRelation as NewBioconceptRelation
    from model_new_schema.phenotype import Phenotype as NewPhenotype
    
    #Cache BioconRelations and phenotypes
    key_to_biocon_relations = cache_by_key(NewBioconceptRelation, new_session, bioconrel_type='PHENOTYPE')
    key_to_phenotype = cache_by_key(NewPhenotype, new_session)
    
    #Create new biocon_relations if they don't exist, or update the database if they do.
    bioconrels = []
    for cv_term in old_cv_terms:
        child_phenotype_key = create_phenotype_key(cv_term.name)
        if child_phenotype_key in key_to_phenotype:
            child_id = key_to_phenotype[child_phenotype_key].id
            for parent in cv_term.parents:
                parent_phenotype_key = create_phenotype_key(parent.name)
                if parent_phenotype_key in key_to_phenotype:
                    parent_id = key_to_phenotype[parent_phenotype_key].id
                    biocon_biocon = NewBioconceptRelation(parent_id, child_id, 'PHENOTYPE', 'is a')
                    bioconrels.append(biocon_biocon)
    success = create_or_update_and_remove(bioconrels, key_to_biocon_relations, [], new_session)
    return success

if __name__ == "__main__":
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(old_session_maker, new_session_maker, False)
    
    
