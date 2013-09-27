from datetime import datetime
from schema_conversion import check_value
from schema_conversion.output_manager import write_to_output_file, OutputCreator
from sqlalchemy.exc import IntegrityError
import logging
import sys

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
        
def set_up_logging(label):
    logging.basicConfig(format='%(asctime)s %(name)s: %(message)s', level=logging.DEBUG, datefmt='%m/%d/%Y %H:%M:%S')
    
    log = logging.getLogger(label)
    
    hdlr = logging.FileHandler('/Users/kpaskov/Documents/Schema Conversion Logs/' + label + '.' + str(datetime.now()) + '.txt')
    formatter = logging.Formatter('%(asctime)s %(name)s: %(message)s', '%m/%d/%Y %H:%M:%S')
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr) 
    log.setLevel(logging.DEBUG)
    return log



