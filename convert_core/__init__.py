from schema_conversion import ask_to_commit, commit_without_asking, check_values, \
    check_value
from schema_conversion.output_manager import write_to_output_file, OutputCreator
from sqlalchemy.exc import IntegrityError
import datetime
import logging
import sys

def execute_conversion(new_session_maker, grab_current_objs_f, conversion_fs, ask):
    log = logging.getLogger(__name__)
    log.info("Conversion started.")
    
    try:
        marked_current_obj_ids = set()
    
        #Execute a series of operations (creating new ones, etc)
        for conversion_f in conversion_fs:
            marked_current_obj_ids.update(conversion_f(new_session_maker, grab_current_objs_f, ask))
    
        #Delete any untouched objects
        new_session = new_session_maker()
        current_objs = grab_current_objs_f(new_session)
        
        for current_obj in current_objs:
            if current_obj.id not in marked_current_obj_ids:
                new_session.delete(current_obj)
    
        #Finish and commit.
        new_session.commit()
        
    except Exception:
        log.exception( "Unexpected error:" + str(sys.exc_info()[0]))
        raise
    finally:
        new_session.close()
        
    log.info("Conversion finished.")
    
def create_or_update(new_obj, current_obj_by_id, current_obj_by_key, values_to_check, session, output_creator):
    #If there's an object with the same key and it also has the same id, then that's our object - we just need to
    #check to make sure it's values match ours.
    if current_obj_by_key is not None and (new_obj.id is None or current_obj_by_key.id == new_obj.id):
        for value_to_check in values_to_check:
            if not check_value(new_obj, current_obj_by_key, value_to_check):
                output_creator.changed(current_obj_by_key.unique_key(), value_to_check)   
        return False 
    else:
        if current_obj_by_id is not None:
            session.delete(current_obj_by_id)
            print 'Removed ' + str(new_obj.id)
            output_creator.removed()
            session.commit()
        
        if current_obj_by_key is not None:
            session.delete(current_obj_by_key)
            print 'Removed' + str(new_obj.unique_key())
            output_creator.removed()
            session.commit()
            
        session.add(new_obj)
        output_creator.added()
        return True
        
def convert_from_bud(old_session_maker, grab_old_objs_f, create_f, values_to_check):
    def f(new_session_maker, grab_current_objs_f, ask):
        try:
            #Grab all current objects
            new_session = new_session_maker()
            current_objs = grab_current_objs_f(new_session)
            id_to_current_obj = dict([(x.id, x) for x in current_objs])
            key_to_current_obj = dict([(x.unique_key(), x) for x in current_objs])
            
            #Grab all old objects
            old_session = old_session_maker()
            old_objs = grab_old_objs_f(old_session)
            
            output_creator = OutputCreator(logging.getLogger(__name__))
            
            for old_obj in old_objs:
                #Convert old objects into new ones
                newly_created_objs = create_f(old_obj)
                
                if newly_created_objs is not None:
                    #Edit or add new objects
                    for newly_created_obj in newly_created_objs:
                        current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                        current_obj_by_key = None if newly_created_obj.unique_key() not in key_to_current_obj else key_to_current_obj[newly_created_obj.unique_key()]
                        create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_key, values_to_check, new_session, output_creator)
            new_session.commit()
    
        except Exception:
            log = logging.getLogger(__name__)
            log.exception( "Unexpected error:" + str(sys.exc_info()[0]))
            raise
        finally:
            new_session.close()
            old_session.close()
    return f


