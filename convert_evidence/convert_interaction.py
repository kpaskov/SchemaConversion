'''
Created on May 6, 2013

@author: kpaskov
'''
from convert_aux.auxillary_tables import create_interaction_precomp, create_interaction, \
    create_interaction_id, create_interaction_family_precomp, \
    create_interaction_family, create_interaction_family_id, create_bioent_reference, \
    create_bioent_reference_id
from schema_conversion import prepare_schema_connection, create_format_name, \
    execute_conversion, new_config, old_config, execute_aux, \
    cache_by_id
from schema_conversion.convert_phenotype import create_phenotype_key
from schema_conversion.output_manager import write_to_output_file
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

def create_geninterevidence_id(old_evidence_id):
    return old_evidence_id - 397921 + 10000000

def create_physinterevidence_id(old_evidence_id):
    return old_evidence_id - 397664 + 20000000

def create_genetic_interevidence(old_interaction, key_to_experiment, key_to_phenotype,
                         reference_ids, bioent_ids):
    from model_new_schema.interaction import Geninteractionevidence as NewGeninteractionevidence
    
    if old_interaction.interaction_type == 'genetic interactions':
        evidence_id = create_geninterevidence_id(old_interaction.id)
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
        
        new_genetic_interevidence = NewGeninteractionevidence(evidence_id, experiment_id, reference_id, None, old_interaction.source, 
                                                            bioent1_id, bioent2_id, phenotype_id, 
                                                            old_interaction.annotation_type, bait_hit, note,
                                                            old_interaction.date_created, old_interaction.created_by)
        return [new_genetic_interevidence]    
    return None

def create_physical_interevidence(old_interaction, key_to_experiment,
                         reference_ids, bioent_ids):
    from model_new_schema.interaction import Physinteractionevidence as NewPhysinteractionevidence
    if old_interaction.interaction_type == 'physical interactions':    
        evidence_id = create_physinterevidence_id(old_interaction.id)

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
            
        new_physical_interevidence = NewPhysinteractionevidence(evidence_id, experiment_id, reference_id, None, old_interaction.source,
                                                             bioent1_id, bioent2_id,
                                                             old_interaction.annotation_type,  old_interaction.modification, bait_hit, note,
                                                             old_interaction.date_created, old_interaction.created_by)
        return [new_physical_interevidence]
    return None

  
"""
---------------------Convert------------------------------
"""  

def convert(old_session_maker, new_session_maker, ask=True):

    from model_old_schema.interaction import Interaction as OldInteraction
    from model_new_schema.reference import Reference as NewReference
    from model_new_schema.interaction import Geninteractionevidence as NewGeninteractionevidence, Physinteractionevidence as NewPhysinteractionevidence
    from model_new_schema.bioentity import Bioentity as NewBioentity
    from model_new_schema.evelement import Experiment as NewExperiment
    from model_new_schema.phenotype import Phenotype as NewPhenotype
    from model_new_schema.auxiliary import Geninteraction as NewGeninteraction, Physinteraction as NewPhysinteraction, \
    Interaction as NewInteraction, InteractionFamily as NewInteractionFamily, \
    GeninteractionBioentityReference as NewGeninteractionBioentityReference, \
    PhysinteractionBioentityReference as NewPhysinteractionBioentityReference
    from model_new_schema.evidence import Evidence as NewEvidence
    
#    get_old_obj_query = lambda old_session: old_session.query(OldInteraction).options(
#                                                            joinedload('interaction_references'),
#                                                            joinedload('interaction_phenotypes'),
#                                                            joinedload('feature_interactions'))
#    
#    # Convert genetic interaction evidence
#    write_to_output_file('Genetic Interaction Evidence')
#    values_to_check = ['experiment_id', 'reference_id', 'strain_id', 'source',
#                       'bioentity1_id', 'bioentity2_id', 'phenotype_id', 
#                       'note', 'annotation_type', 'date_created', 'created_by']
#    execute_conversion(NewGeninteractionevidence, OldInteraction, create_genetic_interevidence, get_old_obj_query,
#                       create_geninterevidence_id, values_to_check, old_session_maker, new_session_maker, 10000, ask,
#                       key_to_experiment = lambda new_session: cache_by_key(NewExperiment, new_session),
#                       key_to_phenotype = lambda new_session: cache_by_key(NewPhenotype, new_session),
#                       reference_ids = lambda new_session: cache_ids(NewReference, new_session),
#                       bioent_ids = lambda new_session: cache_ids(NewBioentity, new_session))
#    
#    # Convert physical interaction evidence
#    write_to_output_file('Physical Interaction Evidence')
#    values_to_check = ['experiment_id', 'reference_id', 'strain_id', 'source',
#                       'bioentity1_id', 'bioentity2_id',
#                       'modification', 'note', 'annotation_type', 'date_created', 'created_by']
#    execute_conversion(NewPhysinteractionevidence, OldInteraction, create_physical_interevidence, get_old_obj_query,
#                       create_physinterevidence_id, values_to_check, old_session_maker, new_session_maker, 10000, ask,
#                       key_to_experiment = lambda new_session: cache_by_key(NewExperiment, new_session),
#                       reference_ids = lambda new_session: cache_ids(NewReference, new_session),
#                       bioent_ids = lambda new_session: cache_ids(NewBioentity, new_session))
#      
#
#    # Create interactions for genetic_interactions
#    write_to_output_file('Genetic Interactions')
#    values_to_check = ['display_name', 'bioentity1_id', 'bioentity2_id', 'evidence_count']
#    execute_aux(NewGeninteraction, NewGeninteractionevidence, create_interaction_precomp, create_interaction,
#                create_interaction_id, values_to_check, new_session_maker, 10000, ask,
#                id_to_bioent = lambda new_session: cache_by_id(NewBioentity, new_session))
#    
#    # Create interactions for physical_interactions
#    write_to_output_file('Physical Interactions')
#    values_to_check = ['display_name', 'bioentity1_id', 'bioentity2_id', 'evidence_count']
#    execute_aux(NewPhysinteraction, NewPhysinteractionevidence, create_interaction_precomp, create_interaction,
#                create_interaction_id, values_to_check, new_session_maker, 10000, ask,
#                id_to_bioent = lambda new_session: cache_by_id(NewBioentity, new_session))
   
    # Create interaction families
    write_to_output_file('Interaction Families')
    values_to_check = ['genetic_ev_count', 'physical_ev_count', 'evidence_count']
    execute_aux(NewInteractionFamily, NewInteraction, create_interaction_family_precomp, create_interaction_family,
                create_interaction_family_id, values_to_check, new_session_maker, 100, ask,
                max_neighbors = lambda new_session: 100,
                id_to_bioent = lambda new_session: cache_by_id(NewBioentity, new_session))
    
    # Create genetic bioent references
    write_to_output_file('Genetic Interaction Bioentity References')
    values_to_check = []
    execute_aux(NewGeninteractionBioentityReference, NewGeninteractionevidence, None, create_bioent_reference,
                create_bioent_reference_id, values_to_check, new_session_maker, 10000, ask,
                bioent_f = lambda new_session: lambda x: [x.bioent1_id, x.bioent2_id])
    
    # Create physical bioent references
    write_to_output_file('Physical Interaction Bioentity References')
    values_to_check = []
    execute_aux(NewPhysinteractionBioentityReference, NewPhysinteractionevidence, None, create_bioent_reference,
                create_bioent_reference_id, values_to_check, new_session_maker, 10000, ask,
                bioent_f = lambda new_session: lambda x: [x.bioent1_id, x.bioent2_id])
        

if __name__ == "__main__":
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(old_session_maker, new_session_maker, False)
   
    
