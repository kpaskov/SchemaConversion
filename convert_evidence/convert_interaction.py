'''
Created on May 6, 2013

@author: kpaskov
'''
from convert_perf.auxillary_tables import convert_interactions, \
    convert_interaction_families, convert_bioent_references
from schema_conversion import create_or_update_and_remove, \
    prepare_schema_connection, create_format_name, cache_by_key_in_range, \
    execute_conversion, new_config, old_config, cache_by_key, cache_ids
from schema_conversion.convert_phenotype import create_phenotype_key
from schema_conversion.output_manager import write_to_output_file
from sqlalchemy.orm import joinedload
import model_new_schema
import model_old_schema

'''
 This code is used to convert interaction data from the old schema to the new. It does this by
 creating new schema objects from the old, then comparing these new objects to those already
 stored in the new database. If a newly created object matches one that is already stored, the two
 are compared and the database fields are updated. If a newly created object does not match one that is 
 already stored, it is added to the database.
'''
"""
------------ Create --------------
"""

def create_interevidence_id(old_evidence_id):
    return old_evidence_id

def create_genetic_interevidence(old_interaction, key_to_experiment, key_to_phenotype,
                         reference_ids, bioent_ids):
    from model_new_schema.interaction import GeneticInterevidence as NewGeneticInterevidence
    
    if old_interaction.interaction_type == 'genetic interactions':
        evidence_id = create_interevidence_id(old_interaction.id)
        reference_ids = old_interaction.reference_ids
        if len(reference_ids) != 1:
            print 'Too many references'
            return None
        reference_id = reference_ids[0]
        if reference_id not in reference_ids:
            print 'Reference does not exist.'
            return None
        note = old_interaction.interaction_references[0].note
        
        bioent_ids = list(old_interaction.feature_ids)
        bioent_ids.sort()
        bioent1_id = bioent_ids[0]
        bioent2_id = bioent_ids[1]
        
        if bioent1_id > bioent2_id:
            print 'Out of order.'
            return None
        if bioent1_id not in bioent_ids:
            print 'Bioentity does not exist.'
            return None
        if bioent2_id not in bioent_ids:
            print 'Bioentity does not exist.'
            return None
        
        old_phenotypes = old_interaction.interaction_phenotypes
        phenotype_id = None
        if len(old_phenotypes) == 1:
            old_phenotype = old_phenotypes[0].phenotype
            phenotype_key = create_phenotype_key(old_phenotype.observable, old_phenotype.qualifier, old_phenotype.mutant_type)
            
            if phenotype_key not in key_to_phenotype:
                print 'Phenotype does not exist. ' + str(phenotype_key)
                return None
            phenotype_id = key_to_phenotype[phenotype_key].id
        elif len(old_phenotypes) > 1:
            print 'Too many phenotypes.'
            return None
        
        
        experiment_key = create_format_name(old_interaction.experiment_type)
        if experiment_key not in key_to_experiment:
            print 'Experiment does not exist. ' + str(experiment_key)
            return None
        experiment_id = key_to_experiment[experiment_key].id
        
        feat_interacts = sorted(old_interaction.feature_interactions, key=lambda x: x.feature_id)
        bait_hit = '-'.join([x.action for x in feat_interacts])
        
        new_genetic_interevidence = NewGeneticInterevidence(evidence_id, experiment_id, reference_id, None, old_interaction.source, 
                                                            bioent1_id, bioent2_id, phenotype_id, 
                                                            old_interaction.annotation_type, bait_hit, note,
                                                            old_interaction.date_created, old_interaction.created_by)
        return new_genetic_interevidence    
    return None

def create_physical_interevidence(old_interaction, key_to_experiment,
                         reference_ids, bioent_ids):
    from model_new_schema.interaction import PhysicalInterevidence as NewPhysicalInterevidence
    if old_interaction.interaction_type == 'physical interactions':    
        evidence_id = create_interevidence_id(old_interaction.id)

        reference_ids = old_interaction.reference_ids
        if len(reference_ids) != 1:
            print 'Too many references'
            return None
        reference_id = reference_ids[0]
        note = old_interaction.interaction_references[0].note
        
        if reference_id not in reference_ids:
            print 'Reference does not exist.'
            return None
        
        bioent_ids = list(old_interaction.feature_ids)
        bioent_ids.sort()
        bioent1_id = bioent_ids[0]
        bioent2_id = bioent_ids[1]
        
        if bioent1_id > bioent2_id:
            print 'Out of order.'
            return None
        if bioent1_id not in bioent_ids:
            print 'Bioentity does not exist.'
            return None
        if bioent2_id not in bioent_ids:
            print 'Bioentity does not exist.'
            return None
        
        experiment_key = create_format_name(old_interaction.experiment_type)
        if experiment_key not in key_to_experiment:
            print 'Experiment does not exist. ' + str(experiment_key)
            return None
        experiment_id = key_to_experiment[experiment_key].id
            
        feat_interacts = sorted(old_interaction.feature_interactions, key=lambda x: x.feature_id)
        bait_hit = '-'.join([x.action for x in feat_interacts])
            
        new_genetic_interevidence = NewPhysicalInterevidence(evidence_id, experiment_id, reference_id, None, old_interaction.source,
                                                             bioent1_id, bioent2_id,
                                                             old_interaction.annotation_type,  old_interaction.modification, bait_hit, note,
                                                             old_interaction.date_created, old_interaction.created_by)
        return new_genetic_interevidence  
    return None

  
"""
---------------------Convert------------------------------
"""  

def convert(old_session_maker, new_session_maker, ask=True):

    from model_old_schema.interaction import Interaction as OldInteraction
    from model_new_schema.reference import Reference as NewReference
    from model_new_schema.interaction import GeneticInterevidence as NewGeneticInterevidence, PhysicalInterevidence as NewPhysicalInterevidence
    
    intervals = [300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000, 1100000, 1200000, 1300000, 1400000]
        
    from model_new_schema.bioentity import Bioentity as NewBioentity
    from model_new_schema.evelement import Experiment as NewExperiment
    from model_new_schema.phenotype import Phenotype as NewPhenotype
    new_session = new_session_maker()
    key_to_experiment = cache_by_key(NewExperiment, new_session)
    key_to_phenotype = cache_by_key(NewPhenotype, new_session)
    reference_ids = cache_ids(NewReference, new_session)
    bioent_ids = cache_ids(NewBioentity, new_session)
        
    # Convert genetic interevidences
    write_to_output_file('GeneticInterevidences')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Interaction ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_genetic_interevidences, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       key_to_experiment = lambda old_session: key_to_experiment,
                       key_to_phenotype = lambda old_session: key_to_phenotype,
                       reference_ids = lambda old_session: reference_ids,
                       bioent_ids = lambda old_session: bioent_ids,
                       old_interactions=lambda old_session: old_session.query(OldInteraction).filter(
                                                            OldInteraction.id >= min_id).filter(
                                                            OldInteraction.id < max_id).options(
                                                            joinedload('interaction_references'),
                                                            joinedload('interaction_phenotypes'),
                                                            joinedload('feature_interactions')).all())
      
    # Convert physical interevidences
    write_to_output_file( 'PhysicalInterevidences')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Interaction ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_physical_interevidences, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       key_to_experiment = lambda old_session: key_to_experiment,
                       reference_ids = lambda old_session: reference_ids,
                       bioent_ids = lambda old_session: bioent_ids,
                       old_interactions=lambda old_session: old_session.query(OldInteraction).filter(
                                                            OldInteraction.id >= min_id).filter(
                                                            OldInteraction.id < max_id).options(
                                                            joinedload('interaction_references'),
                                                            joinedload('feature_interactions')).all())
      
    # Create interactions for genetic_interactions
    write_to_output_file( 'Genetic interactions')
    execute_conversion(convert_interactions, old_session_maker, new_session_maker, ask,
                       interaction_type = lambda old_session : 'GENETIC_INTERACTION',
                       evidence_cls = lambda old_session : NewGeneticInterevidence)
    
    # Create interactions for physical_interactions
    write_to_output_file( 'Physical interactions')
    execute_conversion(convert_interactions, old_session_maker, new_session_maker, ask,
                       interaction_type = lambda old_session : 'PHYSICAL_INTERACTION',
                       evidence_cls = lambda old_session : NewPhysicalInterevidence)
   
    intervals = [0, 50, 500, 1000, 1500, 2000, 2500, 3000, 3500, 
                 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000]
    
    # Create interaction families
    write_to_output_file( 'Interaction families')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_interaction_families, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       max_neighbors = lambda old_session:100,
                       interaction_types = lambda old_session:['GENETIC_INTERACTION', 'PHYSICAL_INTERACTION'])
        
    genetic_evidences = cache_by_key(NewGeneticInterevidence, new_session).values()
    # Create bioent_reference
    write_to_output_file( 'Genetic Interaction Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: genetic_evidences,
                       bioent_ref_type = lambda old_session: 'GENETIC_INTERACTION_EVIDENCE',
                       bioent_f = lambda old_session: lambda x: [x.bioent1_id, x.bioent2_id])
        
     
    physical_evidences = cache_by_key(NewPhysicalInterevidence, new_session).values()   
    # Create bioent_reference
    write_to_output_file( 'Physical Interaction Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: physical_evidences,
                       bioent_ref_type = lambda old_session: 'PHYSICAL_INTERACTION_EVIDENCE',
                       bioent_f = lambda old_session: lambda x: [x.bioent1_id, x.bioent2_id])
                
def convert_genetic_interevidences(new_session, old_interactions, key_to_experiment, key_to_phenotype,
                                   reference_ids, bioent_ids, min_id, max_id):
    '''
    Convert Genetic Interevidences
    '''
    from model_new_schema.interaction import GeneticInterevidence as NewGeneticInterevidence

    
    #Cache interevidences
    key_to_interevidence = cache_by_key_in_range(NewGeneticInterevidence, NewGeneticInterevidence.id, new_session, min_id, max_id)

    #Create new genetic interevidences if they don't exist, or update the database if they do.    
    new_genetic_interevidences = [create_genetic_interevidence(x, key_to_experiment, key_to_phenotype, reference_ids, bioent_ids)
                            for x in old_interactions]
   
    values_to_check = ['experiment_id', 'reference_id', 'strain_id', 'source',
                       'bioent1_id', 'bioent2_id', 'phenotype_id', 
                       'note', 'annotation_type', 'date_created', 'created_by']
    success = create_or_update_and_remove(new_genetic_interevidences, key_to_interevidence, values_to_check, new_session)
    return success

def convert_physical_interevidences(new_session, old_interactions, key_to_experiment,
                                   reference_ids, bioent_ids, min_id, max_id):
    '''
    Convert Physical Interevidences
    '''
    from model_new_schema.interaction import PhysicalInterevidence as NewPhysicalInterevidence
    
    #Cache interevidences
    key_to_interevidence = cache_by_key_in_range(NewPhysicalInterevidence, NewPhysicalInterevidence.id, new_session, min_id, max_id)

    #Create new physical interevidences if they don't exist, or update the database if they do.    
    new_physical_interevidences = [create_physical_interevidence(x, key_to_experiment, reference_ids, bioent_ids)
                            for x in old_interactions]
   
    values_to_check = ['experiment_id', 'reference_id', 'strain_id', 'source',
                       'bioent1_id', 'bioent2_id',
                       'modification', 'note', 'annotation_type', 'date_created', 'created_by']
    success = create_or_update_and_remove(new_physical_interevidences, key_to_interevidence, values_to_check, new_session)
    return success

if __name__ == "__main__":
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(old_session_maker, new_session_maker, False)
   
    