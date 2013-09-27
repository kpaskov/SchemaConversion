'''
Created on Jul 3, 2013

@author: kpaskov
'''

from convert_core import convert_reference, convert_bioentity, \
    convert_evelements, convert_protein
from convert_evidence import convert_interaction, convert_literature, \
    convert_regulation
from schema_conversion import new_config, old_config, prepare_schema_connection
import logging
import model_new_schema
import model_old_schema
import sys

if __name__ == "__main__":    
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    
    #Evelement
    try:
        convert_evelements.convert(old_session_maker, new_session_maker, ask=False)
    except Exception:
        log = logging.getLogger('convert.evelements')
        log.error( "Unexpected error:" + str(sys.exc_info()[0]) )
    
    #Reference
    try:
        convert_reference.convert(old_session_maker, new_session_maker, ask=False)
    except Exception:
        log = logging.getLogger('convert.reference')
        log.error( "Unexpected error:" + str(sys.exc_info()[0]) )
        
    #Bioentity
    try:
        convert_bioentity.convert(old_session_maker, new_session_maker, ask=False)  
    except Exception:
        log = logging.getLogger('convert.bioentity')
        log.error( "Unexpected error:" + str(sys.exc_info()[0]) )
        
    #Protein
    try:
        convert_protein.convert(old_session_maker, new_session_maker, ask=False)  
    except Exception:
        log = logging.getLogger('convert.protein')
        log.error( "Unexpected error:" + str(sys.exc_info()[0]) )
        
    #Regulation
    try:
        convert_regulation.convert(old_session_maker, new_session_maker, ask=False)  
    except Exception:
        log = logging.getLogger('convert.regulation')
        log.error( "Unexpected error:" + str(sys.exc_info()[0]) )
        
    #Phenotype
    
    #Interaction
    try:
        convert_interaction.convert(old_session_maker, new_session_maker, ask=False)
    except Exception:
        log = logging.getLogger('convert.interaction')
        log.error( "Unexpected error:" + str(sys.exc_info()[0]) )
        
    #Literature
    try:
        convert_literature.convert(old_session_maker, new_session_maker, ask=False)
    except Exception:
        log = logging.getLogger('convert.literature')
        log.error( "Unexpected error:" + str(sys.exc_info()[0]) )
        
    #GO
    
    
    