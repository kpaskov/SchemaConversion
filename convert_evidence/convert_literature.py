'''
Created on Jul 3, 2013

@author: kpaskov
'''
from convert_aux.auxillary_tables import convert_bioent_references
from schema_conversion import create_or_update_and_remove, \
    prepare_schema_connection, execute_conversion, cache_by_key_in_range, \
    new_config, cache_by_key, old_config, cache_ids
from schema_conversion.output_manager import write_to_output_file
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.expression import or_
import model_new_schema
import model_old_schema

def create_litevidence_id(old_litevidence_id):
    return old_litevidence_id - 243284 + 30000000

def create_litevidence(old_litevidence, reference_ids, bioent_ids):
    from model_new_schema.literature import Literatureevidence as NewLiteratureevidence
    
    reference_id = old_litevidence.litguide.reference_id
    if reference_id not in reference_ids:
        print 'Reference does not exist. ' + str(reference_id)
        return None
    
    bioentity_id = old_litevidence.feature_id
    if bioentity_id not in bioent_ids:
        return None
    
    new_id = create_litevidence_id(old_litevidence.id)
    topic = old_litevidence.litguide.topic
    
    new_bioentevidence = NewLiteratureevidence(new_id, reference_id, bioentity_id, topic,
                                           old_litevidence.date_created, old_litevidence.created_by)
    return new_bioentevidence

def convert(old_session_maker, new_session_maker, ask=True):
    from model_old_schema.reference import LitguideFeat as OldLitguideFeat
    
    intervals = [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
    
    from model_new_schema.bioentity import Bioentity as NewBioentity
    from model_new_schema.reference import Reference as NewReference
    from model_new_schema.literature import Literatureevidence as NewLiteratureevidence
    new_session = new_session_maker()
    reference_ids = cache_ids(NewReference, new_session)
    bioent_ids = cache_ids(NewBioentity, new_session)
    
    
    # Convert Litguide Evidence
    write_to_output_file( 'Literature Evidence')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioentevidence, old_session_maker, new_session_maker, ask,
                min_id = lambda old_session: min_id,
                max_id = lambda old_session: max_id,
                reference_ids = lambda old_session: reference_ids,
                bioent_ids = lambda old_session: bioent_ids,
                old_litevidence= lambda old_session: old_session.query(OldLitguideFeat).filter(
                            or_(OldLitguideFeat.topic=='Additional Literature',
                                OldLitguideFeat.topic=='Primary Literature',
                                OldLitguideFeat.topic=='Omics',
                                OldLitguideFeat.topic=='Reviews')).options(joinedload('litguide')).filter(
                            OldLitguideFeat.feature_id >=min_id).filter(
                            OldLitguideFeat.feature_id < max_id).all())
        
    litevidences = cache_by_key(NewLiteratureevidence, new_session).values()
    primary_litevidences = [x for x in litevidences if x.topic=='Primary Literature']
    additional_litevidences = [x for x in litevidences if x.topic=='Additional Literature']
    omics_litevidences = [x for x in litevidences if x.topic=='Omics']
    review_litevidences = [x for x in litevidences if x.topic=='Reviews']
    
    # Create bioent_reference
    write_to_output_file( 'Primary Literature Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: primary_litevidences,
                       bioent_ref_type = lambda old_session: 'PRIMARY_LITERATURE',
                       bioent_f = lambda old_session: lambda x: [x.bioent_id])
        
    # Create bioent_reference
    write_to_output_file( 'Additional Literature Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: additional_litevidences,
                       bioent_ref_type = lambda old_session: 'ADDITIONAL_LITERATURE',
                       bioent_f = lambda old_session: lambda x: [x.bioent_id])
        
    # Create bioent_reference
    write_to_output_file( 'Review Literature Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: review_litevidences,
                       bioent_ref_type = lambda old_session: 'REVIEW_LITERATURE',
                       bioent_f = lambda old_session: lambda x: [x.bioent_id])
        
    # Create bioent_reference
    write_to_output_file( 'Omics Literature Bioent_References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file( 'Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioent_references, old_session_maker, new_session_maker, ask,
                       min_id = lambda old_session : min_id,
                       max_id = lambda old_session : max_id,
                       evidences = lambda old_session: omics_litevidences,
                       bioent_ref_type = lambda old_session: 'OMICS_LITERATURE',
                       bioent_f = lambda old_session: lambda x: [x.bioent_id])
        
def convert_bioentevidence(new_session, old_litevidence, reference_ids, bioent_ids, min_id, max_id):
    '''
    Convert Bioentevidence
    '''
    from model_new_schema.literature import Literatureevidence as NewLiteratureEvidence

    #Cache litevidence
    key_to_litevidence = cache_by_key_in_range(NewLiteratureEvidence, NewLiteratureEvidence.bioent_id, new_session, min_id, max_id)
    
    #Create new bioentevidence if they don't exist, or update the database if they do.
    new_litevidence = [create_litevidence(x, reference_ids, bioent_ids) for x in old_litevidence]
    
    values_to_check = ['experiment_id', 'reference_id', 'evidence_type', 'strain_id',
                       'source', 'topic', 'bioentity_id', 'date_created', 'created_by']
    success = create_or_update_and_remove(new_litevidence, key_to_litevidence, values_to_check, new_session)    
    return success

if __name__ == "__main__":
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(old_session_maker, new_session_maker, False)