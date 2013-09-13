'''
Created on Feb 27, 2013

@author: kpaskov
'''
from convert_aux.auxillary_tables import update_biocon_gene_counts, \
    convert_biofact, convert_biocon_ancestors, convert_bioent_references
from schema_conversion import create_or_update_and_remove, \
    prepare_schema_connection, cache_by_key, create_format_name, execute_conversion, \
    new_config, old_config, cache_ids
from schema_conversion.output_manager import write_to_output_file
from utils.link_maker import biocon_link
import model_new_schema
import model_old_schema


"""
---------------------Create------------------------------
"""

def create_go_id(old_go_id):
    return old_go_id + 50000000

def create_goevidence_id(old_evidence_id):
    return old_evidence_id + 50000000

def create_go_key(go_term):
    name = go_term.replace(' ', '_')
    name = name.replace('/', '-')
    return (name, 'GO')
                 
abbrev_to_go_aspect = {'C':'cellular component', 'F':'molecular function', 'P':'biological process'}
def create_go(old_go):
    from model_new_schema.go import Go as NewGo
    
    display_name = old_go.go_term
    format_name = create_format_name(display_name)
    link = biocon_link('GO', format_name)
    new_go = NewGo(create_go_id(old_go.id), display_name, format_name, link, old_go.go_definition,
                   old_go.go_go_id, abbrev_to_go_aspect[old_go.go_aspect],  
                   old_go.date_created, old_go.created_by)
    return new_go

def create_synonyms(old_go, key_to_go):
    from model_new_schema.bioconcept import Bioconceptalias as NewBioconceptalias
    go_key = create_go_key(old_go.go_term)
    if go_key not in key_to_go:
        print 'GO term does not exist. ' + str(go_key)
        return []
    biocon_id = key_to_go[go_key].id
    
    new_aliases = [NewBioconceptalias(synonym.go_synonym, biocon_id, 'GO', synonym.date_created, synonym.created_by) for synonym in old_go.synonyms]
    return new_aliases

def create_goevidence(old_go_feature, go_ref, key_to_go, reference_ids, bioent_ids):
    from model_new_schema.go import Goevidence as NewGoevidence
    evidence_id = create_goevidence_id(go_ref.id)
    reference_id = go_ref.reference_id
    if reference_id not in reference_ids:
        print 'Reference does not exist. ' + str(reference_id)
        return None
    
    bioent_id = old_go_feature.feature_id
    if bioent_id not in bioent_ids:
        print 'Bioentity does not exist. ' + str(bioent_id)
        return None
    
    go_key = create_go_key(old_go_feature.go.go_term)
    if go_key not in key_to_go:
        print 'Go term does not exist. ' + str(go_key)
        return None
    biocon_id = key_to_go[go_key].id
    
    qualifier = None
    if go_ref.go_qualifier is not None and go_ref.qualifier is not None:
        qualifier = go_ref.qualifier
    return NewGoevidence(evidence_id, reference_id, old_go_feature.source,
                        old_go_feature.go_evidence, old_go_feature.annotation_type, qualifier, old_go_feature.date_last_reviewed, 
                        bioent_id, biocon_id, go_ref.date_created, go_ref.created_by)
    return None

def create_biocon_relation(go_path, id_to_old_go, key_to_go):
    from model_new_schema.bioconcept import BioconceptRelation as NewBioconceptRelation
    if go_path.generation == 1:
        ancestor = id_to_old_go[go_path.ancestor_id]
        child = id_to_old_go[go_path.child_id]
        
        parent_id = key_to_go[create_go_key(ancestor.go_term)].id
        child_id = key_to_go[create_go_key(child.go_term)].id
        relationship_type = go_path.relationship_type
        return NewBioconceptRelation(parent_id, child_id, 'GO', relationship_type)
    else:
        return None
     
"""
---------------------Convert------------------------------
"""   

def convert(old_session_maker, new_session_maker, ask):
    from model_old_schema.go import Go as OldGo, GoFeature as OldGoFeature, GoPath as OldGoPath
    from model_new_schema.go import Go as NewGo, Goevidence as NewGoevidence
    from model_new_schema.bioconcept import BioconceptRelation as NewBioconceptRelation
      
    # Convert goterms
    write_to_output_file( 'Go terms')
    execute_conversion(convert_goterms, old_session_maker, new_session_maker, ask,
                       old_goterms=lambda old_session: old_session.query(OldGo).all())
        
    # Convert aliases
    write_to_output_file( 'Go term aliases')
    execute_conversion(convert_aliases, old_session_maker, new_session_maker, ask,
                       old_goterms=lambda old_session: old_session.query(OldGo).all())
        
    # Convert goevidences
    write_to_output_file('Goevidences')
    execute_conversion(convert_goevidences, old_session_maker, new_session_maker, ask,
                       old_go_features=lambda old_session: old_session.query(OldGoFeature).all())
        
    # Convert biocon_relations
    write_to_output_file( 'Biocon_relations')  
    execute_conversion(convert_biocon_relations, old_session_maker, new_session_maker, ask,
                       old_go_paths=lambda old_session:old_session.query(OldGoPath).filter(OldGoPath.generation==1).all(),
                       old_goterms=lambda old_session:old_session.query(OldGo).all())     
        
    # Update gene counts
    write_to_output_file( 'Go term gene counts')
    execute_conversion(update_biocon_gene_counts, old_session_maker, new_session_maker, ask,
                       biocon_cls=lambda old_session:NewGo,
                       evidence_cls=lambda old_session:NewGoevidence)   
    
    intervals = [87500, 87750, 88000, 88500, 
                 90000, 90025, 90050, 90100, 90500, 91000, 91500, 92000, 92500, 93500, 95000, 
                 100000, 103000, 103500, 104000, 104500, 104750, 105000, 107500, 107600, 107650, 107675, 107700, 107800, 108000, 109000, 
                 110000, 115000, 
                 120000, 125000, 130000]
    new_session = new_session_maker()
    key_to_evidence = cache_by_key(NewGoevidence, new_session, class_type='GO')
    key_to_bioconrels = cache_by_key(NewBioconceptRelation, new_session, class_type='GO')
    new_session.close()
    
    # Convert biofacts
    write_to_output_file('Go biofacts')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Biocon ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_biofact, old_session_maker, new_session_maker, ask,
                       key_to_evidence = lambda old_session: key_to_evidence,
                       key_to_bioconrels = lambda old_session: key_to_bioconrels,
                       biocon_type = lambda old_session: 'GO',
                       min_id=lambda old_session: min_id,
                       max_id=lambda old_session: max_id)  
  
    # Convert biocon_ancestors
    # For some reason, when I run this several time, it keeps removing a small number of 
    # biocon_ancestors - not clear why.
    write_to_output_file( 'Biocon_ancestors' )
    execute_conversion(convert_biocon_ancestors, old_session_maker, new_session_maker, ask,
                       bioconrel_type=lambda old_session:'GO',
                       num_generations=lambda old_session:5)   
    
    intervals = [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]

    go_evidences = key_to_evidence.values()   
    # Create bioent_reference
    write_to_output_file( 'Go Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: go_evidences,
                       bioent_ref_type = lambda old_session: 'GO',
                       bioent_f = lambda old_session: lambda x: [x.bioent_id])

def convert_goterms(new_session, old_goterms):
    '''
    Convert Goterms
    '''
    from model_new_schema.go import Go as NewGo

    #Cache goterms
    key_to_go = cache_by_key(NewGo, new_session)
    
    #Create new goterms if they don't exist, or update the database if they do.
    new_goterms = [create_go(x) for x in old_goterms]
    
    values_to_check = ['go_go_id', 'go_aspect', 'display_name', 'link', 'description', 'date_created', 'created_by']
    success = create_or_update_and_remove(new_goterms, key_to_go, values_to_check, new_session)
    return success
   
def convert_aliases(new_session, old_goterms):
    '''
    Convert Goterms
    ''' 
    from model_new_schema.go import Go as NewGo
    from model_new_schema.bioconcept import Bioconceptalias as NewBioconceptalias

    #Cache goterm aliases and goterms
    key_to_alias = cache_by_key(NewBioconceptalias, new_session, class_type='GO')
    key_to_go = cache_by_key(NewGo, new_session)    
    
    new_goterm_aliases = []
    for old_goterm in old_goterms:
        new_goterm_aliases.extend(create_synonyms(old_goterm, key_to_go))
        
    #Create new aliases if they don't exist of update the dataset if they do.
    values_to_check = ['alias_type', 'source', 'category', 'class_type', 'date_created', 'created_by']
    success = create_or_update_and_remove(new_goterm_aliases, key_to_alias, values_to_check, new_session)
    return success
    
def convert_goevidences(new_session, old_go_features):
    '''
    Convert Goterms
    '''
    from model_new_schema.go import Goevidence as NewGoevidence, Go as NewGo
    from model_new_schema.bioentity import Bioentity as NewBioentity
    from model_new_schema.reference import Reference as NewReference
    
    #Cache goevidences and goterms
    key_to_goevidence = cache_by_key(NewGoevidence, new_session)
    key_to_go = cache_by_key(NewGo, new_session)
    reference_ids = cache_ids(NewReference, new_session)
    bioent_ids = cache_ids(NewBioentity, new_session)
            
    #Create new goevidences if they don't exist, or update the database if they do.
    new_evidences = []
    values_to_check = ['experiment_id', 'reference_id', 'strain_id', 'source',
                       'go_evidence', 'annotation_type', 'date_last_reviewed', 'qualifier',
                       'bioent_id', 'biocon_id', 'date_created', 'created_by']
    for old_go_feature in old_go_features: 
        new_evidences.extend([create_goevidence(old_go_feature, x, key_to_go, reference_ids, bioent_ids) for x in old_go_feature.go_refs])
    
    success = create_or_update_and_remove(new_evidences, key_to_goevidence, values_to_check, new_session)
    return success      
    
def convert_biocon_relations(new_session, old_go_paths, old_goterms):
    '''
    Convert Biocon_relations
    '''
    from model_new_schema.bioconcept import BioconceptRelation as NewBioconceptRelation
    from model_new_schema.go import Go as NewGo
    
    #Cache biocon_relations and goterms
    key_to_biocon_relations = cache_by_key(NewBioconceptRelation, new_session, bioconrel_type='GO')
    key_to_go = cache_by_key(NewGo, new_session)
    
    id_to_old_go = dict([(x.id, x) for x in old_goterms])
    
    #Create new biocon_relations if they don't exist, or update the database if they do.
    new_biocon_relations = filter(None, [create_biocon_relation(x, id_to_old_go, key_to_go) for x in old_go_paths])
    success = create_or_update_and_remove(new_biocon_relations, key_to_biocon_relations, [], new_session) 
    return success
            
if __name__ == "__main__":
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(old_session_maker, new_session_maker, False)
    

    
            
        
            
            
            
            
            