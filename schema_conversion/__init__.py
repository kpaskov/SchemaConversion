from numbers import Number
from schema_conversion.output_manager import OutputCreator, write_to_output_file
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.types import Float
import datetime
import sys

def prepare_schema_connection(model_cls, config_cls):
    model_cls.SCHEMA = config_cls.SCHEMA
    class Base(object):
        __table_args__ = {'schema': config_cls.SCHEMA, 'extend_existing':True}

    model_cls.Base = declarative_base(cls=Base)
    model_cls.metadata = model_cls.Base.metadata
    engine = create_engine("%s://%s:%s@%s/%s" % (config_cls.DBTYPE, config_cls.DBUSER, config_cls.DBPASS, config_cls.DBHOST, config_cls.DBNAME), convert_unicode=True, pool_recycle=3600)
    model_cls.Base.metadata.bind = engine
    session_maker = sessionmaker(bind=engine)
        
    return session_maker
    
def check_value(new_obj, old_obj, field_name):
    new_obj_value = getattr(new_obj, field_name)
    old_obj_value = getattr(old_obj, field_name)

    if isinstance(new_obj_value, (int, long, float, complex)) and isinstance(old_obj_value, (int, long, float, complex)):
        if not float_approx_equal(new_obj_value, old_obj_value):
            setattr(old_obj, field_name, new_obj_value)
            return False
    elif new_obj_value != old_obj_value:
        setattr(old_obj, field_name, new_obj_value)
        return False
    return True

def check_values(new_obj, old_obj, field_names, output_creator, key):
    for field_name in field_names:
        if not check_value(new_obj, old_obj, field_name):
            output_creator.changed(key, field_name)
    
def add_or_check(new_obj, key_mapping, id_mapping, key, values_to_check, session, output_creator):
    if key in key_mapping:
        current_obj = key_mapping[key]
        check_values(new_obj, current_obj, values_to_check, output_creator, key)
        return False
    else:
        if new_obj.id in id_mapping:
            to_be_removed = id_mapping[new_obj.id]
            session.delete(to_be_removed)
        
        session.add(new_obj)
        key_mapping[key] = new_obj
        id_mapping[new_obj.id] = new_obj
        output_creator.added()
        return True
    
def create_format_name(display_name):
    format_name = display_name.replace(' ', '_')
    format_name = format_name.replace('/', '-')
    return format_name
    
def float_approx_equal(x, y, tol=1e-18, rel=1e-7):
    #http://code.activestate.com/recipes/577124-approximately-equal/
    if tol is rel is None:
        raise TypeError('cannot specify both absolute and relative errors are None')
    tests = []
    if tol is not None: tests.append(tol)
    if rel is not None: tests.append(rel*abs(x))
    assert tests
    return abs(x - y) <= max(tests)

def break_up_file(filename, delimeter='\t'):
    rows = []
    f = open(filename, 'r')
    for line in f:
        rows.append(line.split(delimeter))
    f.close()
    return rows

