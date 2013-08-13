'''
Created on May 31, 2013

@author: kpaskov
'''
from schema_conversion import create_or_update_and_remove, \
    prepare_schema_connection, cache_by_key, cache_by_id, cache_by_key_in_range, \
    execute_conversion, new_config, old_config
from schema_conversion.output_manager import write_to_output_file
from sqlalchemy.orm import joinedload
from utils.link_maker import bioent_link
import model_new_schema
import model_old_schema


"""
---------------------Create------------------------------
"""
other_bioent_types = set(['CHROMOSOME', 'PLASMID', 'ARS', 'CENTROMERE', 'TELOMERE', 
                         'RETROTRANSPOSON'])

locus_bioent_types = {'NCRNA', 'RRNA', 'SNRNA', 'SNORNA', 'TRNA', 'TRANSCRIPTION_FACTOR', 'ORF', 
                         'GENE_CASSETTE', 'MATING_LOCUS', 'MULTIGENE_LOCUS', 'PSEUDOGENE', 'TRANSPOSABLE_ELEMENT_GENE',
                         'NOT_IN_SYSTEMATIC_SEQUENCE_OF_S288C', 'NOT_PHYSICALLY_MAPPED'}

def create_locus_type(old_feature_type):
    bioent_type = old_feature_type.upper()
    bioent_type = bioent_type.replace (" ", "_")
    if bioent_type in locus_bioent_types:
        return bioent_type
    else:
        return None
    
def create_bioent_type(old_feature_type):
    bioent_type = old_feature_type.upper()
    bioent_type = bioent_type.replace (" ", "_")
    if bioent_type in other_bioent_types:
        return bioent_type
    else:
        return None
        
def create_locus(old_bioentity):
    from model_new_schema.bioentity import Locus as NewLocus
    
    locus_type = create_locus_type(old_bioentity.type)
    if locus_type is None:
        return None
    
    display_name = old_bioentity.gene_name
    if display_name is None:
        display_name = old_bioentity.name
    
    format_name = old_bioentity.name.upper()
    link = bioent_link('LOCUS', format_name)
    
    qualifier = None
    attribute = None
    short_description = None
    headline = None
    description = None
    genetic_position = None
    
    ann = old_bioentity.annotation
    if ann is not None:
        qualifier = ann.qualifier
        attribute = ann.attribute
        short_description = ann.name_description
        headline = ann.headline
        description = ann.description
        genetic_position = ann.genetic_position
    
    bioent = NewLocus(old_bioentity.id, display_name, format_name, link, old_bioentity.source, old_bioentity.status, 
                         locus_type, qualifier, attribute, short_description, headline, description, genetic_position, 
                         old_bioentity.date_created, old_bioentity.created_by)
    return bioent 

def create_bioentity(old_bioentity):
    from model_new_schema.bioentity import Bioentity as NewBioentity

    bioent_type = create_bioent_type(old_bioentity.type)
    if bioent_type is None:
        return None
    
    display_name = old_bioentity.gene_name
    if display_name is None:
        display_name = old_bioentity.name
    
    format_name = old_bioentity.name.upper()
    link = bioent_link('BIOENTITY', format_name)
    
    bioent = NewBioentity(old_bioentity.id, 'BIOENTITY', display_name, format_name, link, 
                          old_bioentity.source, old_bioentity.status, 
                          old_bioentity.date_created, old_bioentity.created_by)
    return bioent 

def create_alias(old_alias, id_to_bioentity):
    from model_new_schema.bioentity import BioentAlias as NewBioentAlias

    bioent_id = old_alias.feature_id
    
    if bioent_id is None or not bioent_id in id_to_bioentity:
        #print 'Bioentity does not exist.'
        return None
    
    new_alias = NewBioentAlias(old_alias.alias_name, None, old_alias.alias_type, 
                               bioent_id, old_alias.date_created, old_alias.created_by)
    return new_alias 

def create_dbxref_alias(old_altid, id_to_bioentity):
    from model_new_schema.bioentity import BioentAlias as NewBioentAlias

    bioent_id = old_altid.feature_id
    dbxref = old_altid.dbxref
    
    if bioent_id is None or not bioent_id in id_to_bioentity:
        #print 'Bioentity does not exist.'
        return None
    
    new_alias = NewBioentAlias(dbxref.dbxref_id, dbxref.source, dbxref.dbxref_type, bioent_id, 
                               dbxref.date_created, dbxref.created_by)
    return new_alias 


def create_url(old_url, id_to_bioentity):
    from model_new_schema.bioentity import BioentUrl as NewBioentUrl
    
    url = old_url.url.url
    url_type = old_url.url.url_type
    if url_type == 'query by SGDID':
        url = url.replace('_SUBSTITUTE_THIS_', str(old_url.feature.dbxref_id))
    elif url_type == 'query by SGD ORF name with anchor' or url_type == 'query by SGD ORF name':
        url = url.replace('_SUBSTITUTE_THIS_', str(old_url.feature.name))
    else:
        print "Can't handle this url. " + old_url.url.url_id
        
    display_name = None
    category = None
    for display in old_url.url.displays:
        potential_name = display.label_name
        category = display.label_location
        if potential_name != 'default' and (display_name is None or len(potential_name) > len(display_name)):
            display_name = potential_name

    bioent_id = old_url.feature_id
    if bioent_id not in id_to_bioentity:
        #print 'Bioentity does not exist.'
        return None
    
    new_url = NewBioentUrl(display_name, old_url.url.source, url, category, bioent_id, old_url.url.date_created, old_url.url.created_by)
    return new_url 

def create_dbxref_url(old_altid, id_to_bioentity):
    from model_new_schema.bioentity import BioentUrl as NewBioentUrl
    
    bioent_id = old_altid.feature_id
    if bioent_id not in id_to_bioentity:
        #print 'Bioentity does not exist.'
        return []
            
    new_urls = []
    for old_url in old_altid.dbxref.urls:
        url = old_url.url
        url_type = old_url.url_type
        
        if url_type == 'query by ID assigned by database' or url_type == 'query by SGDID':
            url = url.replace('_SUBSTITUTE_THIS_', str(old_altid.dbxref.dbxref_id))
        else:
            print "Can't handle this url. " + str(old_url.id)
        
        for display in old_url.displays:
            display_name = display.label_name
            category = display.label_location
            new_url = NewBioentUrl(display_name, old_url.source, url, category, bioent_id, old_url.date_created, old_url.created_by)
            new_urls.append(new_url)
    return new_urls

"""
---------------------Convert------------------------------
"""   

def convert(old_session_maker, new_session_maker, ask=True):
    from model_old_schema.feature import Feature as OldFeature, AliasFeature as OldAliasFeature
    from model_old_schema.general import FeatUrl as OldFeatUrl, DbxrefFeat as OldDbxrefFeat
    
    # Convert Locus
    write_to_output_file('Locus')
    execute_conversion(convert_locuses, old_session_maker, new_session_maker, ask,
                       old_bioentity=lambda old_session: old_session.query(OldFeature).options(
                                                        joinedload('annotation')).all())

    # Convert other bioentities
    write_to_output_file('Other Bioentity')
    execute_conversion(convert_other_bioentities, old_session_maker, new_session_maker, ask,
                       old_bioentity=lambda old_session: old_session.query(OldFeature).all())
        
    # Convert aliases
    write_to_output_file('Alias')
    execute_conversion(convert_aliases, old_session_maker, new_session_maker, ask,
                       old_aliases=lambda old_session: old_session.query(OldAliasFeature).options(
                                                        joinedload('alias')).all(),
                       old_altids=lambda old_session: old_session.query(OldDbxrefFeat).options(
                                                        joinedload('dbxref')).all())
        
    # Convert urls
    write_to_output_file('Url')
    intervals = [0, 1000, 2000, 3000, 4000, 5000, 7000, 50000]
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Feature ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_urls, old_session_maker, new_session_maker, ask, 
                        min_id = lambda old_session : min_id,
                        max_id = lambda old_session : max_id,
                        old_urls = lambda old_session: old_session.query(OldFeatUrl).filter(
                                                        OldFeatUrl.feature_id >= min_id).filter(
                                                        OldFeatUrl.feature_id < max_id).options(
                                                        joinedload('url'), 
                                                        joinedload('feature'), 
                                                        joinedload('url.displays')).all(),
                        old_altids=lambda old_session: old_session.query(OldDbxrefFeat).filter(
                                                        OldDbxrefFeat.feature_id >= min_id).filter(
                                                        OldDbxrefFeat.feature_id < max_id).options(
                                                        joinedload('dbxref'), 
                                                        joinedload('dbxref.dbxref_urls'),
                                                        joinedload('dbxref.dbxref_urls.url'),
                                                        joinedload('dbxref.dbxref_urls.url.displays')).all())

def convert_locuses(new_session, old_bioentity=None):
    
    from model_new_schema.bioentity import Locus as NewLocus
    
    #Cache bioentities
    key_to_bioentity = cache_by_key(NewLocus, new_session)
    
    #Create new genes if they don't exist, or update the database if they do. 
    new_bioentities = [create_locus(x) for x in old_bioentity]
    
    values_to_check = ['display_name', 'link', 'source', 'status', 'date_created', 'created_by',
                       'qualifier', 'attribute', 'name_description', 'headline', 'description', 
                       'genetic_position', 'locus_type']
    success = create_or_update_and_remove(new_bioentities, key_to_bioentity, values_to_check, new_session)
    return success
        
def convert_other_bioentities(new_session, old_bioentity=None):
    
    from model_new_schema.bioentity import Bioentity as NewBioentity
    
    #Cache bioentities
    key_to_bioentity = cache_by_key(NewBioentity, new_session, bioent_type='BIOENTITY')
    
    #Create new bioentities if they don't exist, or update the database if they do. 
    new_bioentities = [create_bioentity(x) for x in old_bioentity]
    
    values_to_check = ['display_name', 'link', 'source', 'status', 'date_created', 'created_by']
    success = create_or_update_and_remove(new_bioentities, key_to_bioentity, values_to_check, new_session)
    return success

def convert_aliases(new_session, old_aliases, old_altids=None):
    
    from model_new_schema.bioentity import Bioentity as NewBioentity, BioentAlias as NewBioentAlias
    
    #Cache aliases
    key_to_alias = cache_by_key(NewBioentAlias, new_session)
    id_to_bioentity = cache_by_id(NewBioentity, new_session)
    
    #Create new aliases if they don't exist, or update the database if they do. 
    new_aliases = [create_alias(x, id_to_bioentity) for x in old_aliases]
    new_aliases.extend([create_dbxref_alias(x, id_to_bioentity) for x in old_altids])
    
    values_to_check = ['source', 'category', 'created_by', 'date_created']
    success = create_or_update_and_remove(new_aliases, key_to_alias, values_to_check, new_session)
    return success

def convert_urls(new_session, old_urls=None, old_altids=None, min_id=None, max_id=None):
    
    from model_new_schema.bioentity import Bioentity as NewBioentity, BioentUrl as NewBioentUrl
    
    #Cache urls
    key_to_url = cache_by_key_in_range(NewBioentUrl, NewBioentUrl.bioent_id, new_session, min_id, max_id)
    id_to_bioentity = cache_by_id(NewBioentity, new_session)
    
    #Create new urls if they don't exist, or update the database if they do. 
    new_urls = [create_url(x, id_to_bioentity) for x in old_urls]
    for x in old_altids:
        new_urls.extend(create_dbxref_url(x, id_to_bioentity))
    
    values_to_check = ['display_name', 'source', 'created_by', 'date_created']
    success = create_or_update_and_remove(new_urls, key_to_url, values_to_check, new_session)
    return success

if __name__ == "__main__":
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(old_session_maker, new_session_maker, False)   
   

