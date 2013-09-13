'''
Created on Sep 10, 2013

@author: kpaskov
'''
from convert_aux.auxillary_tables import convert_bioent_references
from schema_conversion import execute_conversion_file, new_config, \
    prepare_schema_connection, cache_by_key, create_format_name, create_or_update, \
    create_or_update_and_remove, break_up_file, cache_references_by_pubmed, \
    cache_by_key_in_range
from schema_conversion.output_manager import write_to_output_file
from utils.link_maker import experiment_link
import model_new_schema

"""
---------------------Create------------------------------
"""  
def create_regevidence_id(old_regevidence_id):
    return old_regevidence_id + 40000000

def create_experiment_id(old_experiment_id):
    return old_experiment_id + 700000

def create_experiment(display_name, row_id):
    from model_new_schema.evelement import Experiment
    
    format_name = create_format_name(display_name)
    link = experiment_link(format_name)

    new_experiment = Experiment(create_experiment_id(row_id), display_name, format_name, link, None, None, None)
    return new_experiment

def create_experiment_alias(experiment_display_name, experiment_eco_id, row_id, key_to_experiment):
    from model_new_schema.evelement import Experimentalias
    
    format_name = create_format_name(experiment_display_name)
    
    if format_name not in key_to_experiment:
        print 'Experiment does not exist ' + str(format_name)
        return None
    experiment_id = key_to_experiment[format_name].id
    
    new_experiment_alias = Experimentalias(experiment_eco_id, 'ECO', 'ECO', experiment_id, None, None)
    return new_experiment_alias

def create_regulationevidence(row, row_id, key_to_experiment, key_to_bioent, pubmed_to_reference):
    from model_new_schema.regulation import Regulationevidence
    
    #bioent1_gene_name = row[0]
    bioent1_format_name = row[1].upper()
    bioent2_format_name = row[3].upper()
    experiment_name = row[4]
    #experiment_eco_id = row[5]
    conditions = row[6]
    #unknown_field1 = row[7]
    #unknown_field2 = row[8]
    #unknown_field3 = row[9]
    pubmed_id = int(row[10])
    source = row[11]
    
    if (bioent1_format_name, 'LOCUS') in key_to_bioent:
        bioent1 = key_to_bioent[(bioent1_format_name, 'LOCUS')]
    elif (bioent1_format_name, 'BIOENTITY') in key_to_bioent:
        bioent1 = key_to_bioent[(bioent1_format_name, 'BIOENTITY')]
    else:
        print 'Bioent does not exist ' + str(bioent1_format_name)
        return None
    bioent1_id = bioent1.id
    
    if (bioent2_format_name, 'LOCUS') in key_to_bioent:
        bioent2 = key_to_bioent[(bioent2_format_name, 'LOCUS')]
    elif (bioent2_format_name, 'BIOENTITY') in key_to_bioent:
        bioent2 = key_to_bioent[(bioent2_format_name, 'BIOENTITY')]
    else:
        print 'Bioent does not exist ' + str(bioent2_format_name)
        return None
    bioent2_id = bioent2.id
    
    experiment_key = create_format_name(experiment_name)
    if experiment_key not in key_to_experiment:
        print 'Experiment does not exist ' + str(experiment_key)
        return None
    experiment_id = key_to_experiment[experiment_key].id
    
    if pubmed_id not in pubmed_to_reference:
        print 'Reference does not exist ' + str(pubmed_id)
        return None
    reference_id = pubmed_to_reference[pubmed_id].id
    
    new_evidence = Regulationevidence(create_regevidence_id(row_id), experiment_id, reference_id, None, source, 
                                      bioent1_id, bioent2_id, conditions, 
                                      None, None)
    return new_evidence
    
  
"""
---------------------Convert------------------------------
"""  

def convert(new_session_maker, ask):
    
    from model_new_schema.regulation import Regulationevidence
    
    rows = break_up_file('/Users/kpaskov/final/combined24042013.txt')
    
    # Convert experiments
    write_to_output_file('Experiments')
    execute_conversion_file(convert_experiments, new_session_maker, ask,
                       data=rows)
    
    # Convert experiment_aliases
    write_to_output_file('Experiment Aliases')
    execute_conversion_file(convert_experiment_aliases, new_session_maker, ask,
                       data=rows)
    
    intervals = [0, 50000, 100000, 150000, 200000, 250000, 300000, 350000, 400000, 450000, 500000, 550000, 600000, 650000]
    # Convert evidence
    write_to_output_file('Regulationevidence')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Evidence ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion_file(convert_regevidence, new_session_maker, ask,
                        min_id=min_id,
                        max_id=max_id,
                        data=rows)  
        
#    # Create interactions for physical_interactions
#    write_to_output_file( 'Regulation interactions')
#    execute_conversion_file(convert_interactions, new_session_maker, ask,
#                       interaction_type = 'REGULATION',
#                       evidence_cls = Regulationevidence)
    
    intervals = [0, 50, 500, 1000, 1500, 2000, 2500, 3000, 3500, 
                 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 10000]
    new_session = new_session_maker()
    regevidences = cache_by_key(Regulationevidence, new_session).values()   
    # Create bioent_reference
    write_to_output_file( 'Regulation Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion_file(convert_bioent_references, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id,
                       evidences = regevidences,
                       bioent_ref_type = 'REGULATION_EVIDENCE',
                       bioent_f = lambda x: [x.bioent1_id, x.bioent2_id])
   

def convert_experiments(new_session, data):
    from model_new_schema.evelement import Experiment
    
    #Cache experiments
    key_to_experiment = cache_by_key(Experiment, new_session)
    
    #Create new experiments if they don't exist, or update the database if they do.   
    experiment_names = set([row[4] for row, row_id in data])
    
    new_experiments = []
    i=0
    for experiment_name in experiment_names:
        new_experiments.append(create_experiment(experiment_name, i))
        i = i+1
    
    values_to_check = ['display_name', 'description', 'date_created', 'created_by']
    success = create_or_update(new_experiments, key_to_experiment, values_to_check, new_session)
    return success

def convert_experiment_aliases(new_session, data):
    from model_new_schema.evelement import Experimentalias, Experiment
    
    #Cache experiment_aliases
    key_to_experiment_alias = cache_by_key(Experimentalias, new_session)
    key_to_experiment = cache_by_key(Experiment, new_session)
    
    #Create new experiments if they don't exist, or update the database if they do.    
    experiment_names = set([(row[4], row[5]) for row, row_id in data])
    
    new_experiment_aliases = []
    i=0
    for experiment_name, eco_name in experiment_names:
        new_experiment_aliases.append(create_experiment_alias(experiment_name, eco_name, i, key_to_experiment))
        i = i+1
   
    values_to_check = ['source', 'category', 'date_created', 'created_by']
    success = create_or_update(new_experiment_aliases, key_to_experiment_alias, values_to_check, new_session)
    return success
    
def convert_regevidence(new_session, min_id, max_id, data):
    from model_new_schema.regulation import Regulationevidence
    from model_new_schema.bioentity import Bioentity
    from model_new_schema.evelement import Experiment
    
    #Cache evidence
    key_to_evidence = cache_by_key_in_range(Regulationevidence, Regulationevidence.id, new_session, create_regevidence_id(min_id), create_regevidence_id(max_id))
    key_to_experiment = cache_by_key(Experiment, new_session)
    key_to_bioent = cache_by_key(Bioentity, new_session)
    pubmed_to_reference = cache_references_by_pubmed(new_session)
    
    #Create new evidences if they don't exist, or update the database if they do.    
    new_evidences = [create_regulationevidence(row, row_id, key_to_experiment, key_to_bioent, pubmed_to_reference)
                            for row, row_id in data[min_id:max_id]]
   
    values_to_check = ['experiment_id', 'reference_id', 'strain_id', 'source', 'conditions', 
                       'bioent1_id', 'bioent2_id', 'date_created', 'created_by']
    success = create_or_update_and_remove(new_evidences, key_to_evidence, values_to_check, new_session)
    return success
    
if __name__ == "__main__":
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(new_session_maker, False)