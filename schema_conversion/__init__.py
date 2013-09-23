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

def cache_by_key(cls, session, **kwargs):
    cache_entries = dict([(x.unique_key(), x) for x in session.query(cls).filter_by(**kwargs).all()])
    return cache_entries

def cache_by_key_limit(cls, session, limit, **kwargs):
    cache_entries = dict([(x.unique_key(), x) for x in session.query(cls).filter_by(**kwargs).limit(limit)])
    return cache_entries

def cache_link_by_key(cls, session, **kwargs):
    cache_entries = dict([(x.unique_key(), (x.id, x.name_with_link)) for x in session.query(cls).filter_by(**kwargs).all()])
    return cache_entries

def cache_name_by_key(cls, session, **kwargs):
    cache_entries = dict([(x.unique_key(), (x.id, x.display_name)) for x in session.query(cls).filter_by(**kwargs).all()])
    return cache_entries

def cache_by_key_in_range(cls, col, session, min_id, max_id, **kwargs):
    cache_entries = dict([(x.unique_key(), x) for x in session.query(cls).filter(col >= min_id).filter(col < max_id).filter_by(**kwargs).all()])
    return cache_entries

def cache_by_id(cls, session, **kwargs):
    cache_entries = dict([(x.id, x) for x in session.query(cls).filter_by(**kwargs).all()])
    return cache_entries

def cache_link_by_id(cls, session, **kwargs):
    cache_entries = dict([(x.id, (x.name_with_link, x.display_name, x.link)) for x in session.query(cls.id, cls.name_with_link, cls.display_name, cls.link).filter_by(**kwargs).all()])
    return cache_entries

def cache_by_id_in_range(cls, col, session, min_id, max_id, **kwargs):
    cache_entries = dict([(x.id, x) for x in session.query(cls).filter(col >= min_id).filter(col < max_id).filter_by(**kwargs).all()])
    return cache_entries

def cache_ids(cls, session, **kwargs):
    cache_ids = [x.id for x in session.query(cls.id).filter_by(**kwargs).all()]
    return cache_ids

def cache_ids_in_range(cls, col, session, min_id, max_id, **kwargs):
    cache_ids = [x.id for x in session.query(cls.id).filter(col >= min_id).filter(col < max_id).filter_by(**kwargs).all()]
    return cache_ids
    
def cache_references_by_pubmed(session, **kwargs):
    from model_new_schema.reference import Reference
    cache_entries = dict([(x.pubmed_id, x) for x in session.query(Reference).filter_by(**kwargs).all()])
    return cache_entries
    
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
    
def create_or_update(new_objs, mapping, values_to_check, session):
    new_objs = filter(None, new_objs)
    output_creator = OutputCreator()
    
    to_be_added = set([new_obj.id for new_obj in new_objs if new_obj.unique_key() not in mapping])
    problem_objs = [old_obj for old_obj in mapping.values() if old_obj.id in to_be_added]
    if len(problem_objs) > 0:
        write_to_output_file( str(len(problem_objs)) + ' problem objects exist and must be deleted to continue.' )
        write_to_output_file( [problem.id for problem in problem_objs] )
        for obj in problem_objs:
            session.delete(obj)
        return False
    else:
        # Check old objects or add new objects.
        for new_obj in new_objs:
            key = new_obj.unique_key()
            add_or_check(new_obj, mapping, key, values_to_check, session, output_creator)
            
        output_creator.finished()
        return True
    
def create_or_update_and_remove(new_objs, mapping, values_to_check, session, full_mapping=None):
    if full_mapping is None:
        full_mapping = mapping
    
    new_objs = filter(None, new_objs)
    new_objs.sort()
    output_creator = OutputCreator()
    to_be_removed = set(mapping.keys())
    
    to_be_added = set([new_obj.id for new_obj in new_objs if new_obj.unique_key() not in mapping])
    problem_objs = [old_obj for old_obj in full_mapping.values() if old_obj.id in to_be_added]
    if len(problem_objs) > 0:
        write_to_output_file( str(len(problem_objs)) + ' problem objects exist and must be deleted to continue.' )
        write_to_output_file( [problem.id for problem in problem_objs] )
        for obj in problem_objs:
            session.delete(obj)
        return False
    else:
        # Check old objects or add new objects.
        for new_obj in new_objs:
            key = new_obj.unique_key()
            add_or_check(new_obj, full_mapping, key, values_to_check, session, output_creator)
            
            if key in to_be_removed:
                to_be_removed.remove(key)
            
        for r_id in to_be_removed:
            session.delete(mapping[r_id])
            output_creator.removed()
        output_creator.finished()
        return True
    
def ask_to_commit(new_session, start_time):
    pause_begin = datetime.datetime.now()
    user_input = None
    while user_input != 'Y' and user_input != 'N':
        user_input = raw_input('Commit these changes (Y/N)?')
    pause_end = datetime.datetime.now()
    if user_input == 'Y':
        new_session.commit()
    end_time = datetime.datetime.now()
    write_to_output_file( str(end_time - pause_end + pause_begin - start_time) + '\n' )
    
def commit_without_asking(new_session, start_time):
    new_session.commit()
    end_time = datetime.datetime.now()
    write_to_output_file(str(end_time - start_time) + '\n')
    
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

def execute_conversion(new_obj_cls, old_obj_cls, create_f, get_old_obj_query, create_id_f, 
                       values_to_check, old_session_maker, new_session_maker, limit, ask, **kwargs):
    start_time = datetime.datetime.now()
    try:
        old_session = old_session_maker()
        
        new_session = new_session_maker()
        kwargs = dict([(x, y(new_session)) for x, y in kwargs.iteritems()])
        new_session.close()
        
        old_obj_count = get_old_obj_query(old_session).count()
        
        if create_id_f is None:
            key_to_obj = cache_by_key(new_obj_cls, new_session)
        
        end_time = datetime.datetime.now()
        print 'Prep ' + str(end_time - start_time) + '\n'
        
        start_time = end_time
        
        iterations = 0
        min_id = 0
        old_objs = get_old_obj_query(old_session).filter(old_obj_cls.id >= min_id).order_by(old_obj_cls.id).limit(limit).all()
        
        while(len(old_objs) > 0):
            max_id = max([x.id for x in old_objs]) + 1
            print "{0:.2f}%".format(100.0 * (iterations + 1) * limit / old_obj_count)
            if create_id_f is not None:
                print str(min_id) + ' (' + str(create_id_f(min_id)) + ') to ' + str(max_id) + ' (' + str(create_id_f(max_id)) + ')'
            else:
                print str(min_id) + ' to ' + str(max_id)
     
            success = False
            while not success:
                new_session = new_session_maker()
                
                if create_id_f is not None:
                    key_to_obj = cache_by_key_in_range(new_obj_cls, new_obj_cls.id, new_session, create_id_f(min_id), create_id_f(max_id))
            
                new_objs = []
                for old_obj in old_objs:
                    newly_created_objs = create_f(old_obj, **kwargs)
                    if newly_created_objs is not None:
                        new_objs.extend(newly_created_objs)
            
                if create_id_f is None:
                    success = create_or_update(new_objs, key_to_obj, values_to_check, new_session)
                else:
                    success = create_or_update_and_remove(new_objs, key_to_obj, values_to_check, new_session)
            
                if ask:
                    ask_to_commit(new_session, start_time)  
                else:
                    commit_without_asking(new_session, start_time)
                new_session.close()
              
            start_time = datetime.datetime.now()  
            iterations = iterations + 1
            min_id = max_id
            old_objs = get_old_obj_query(old_session).filter(old_obj_cls.id >= min_id).order_by(old_obj_cls.id).limit(limit).all()
            
    except Exception:
        write_to_output_file( "Unexpected error:" + str(sys.exc_info()[0]) )
        raise
    finally:
        old_session.close()
        new_session.close()  
        
def execute_aux(new_obj_cls, old_obj_cls, precomp_f, create_f, create_id_f, 
                       values_to_check, new_session_maker, limit, ask, **kwargs):
    start_time = datetime.datetime.now()
    try:        
        new_session = new_session_maker()
        kwargs = dict([(x, y(new_session)) for x, y in kwargs.iteritems()])
        
        get_old_obj_query = lambda new_session: new_session.query(old_obj_cls)
        old_obj_count = get_old_obj_query(new_session).count()
        
        precomp = None
        if precomp_f is not None:
            old_objs = get_old_obj_query(new_session).all()
            precomp = precomp_f(old_objs, **kwargs)
        new_session.close()
        
        end_time = datetime.datetime.now()
        print 'Prep ' + str(end_time - start_time) + '\n'
        
        start_time = end_time
        
        iterations = 0
        min_id = 0
        old_objs = get_old_obj_query(new_session).filter(old_obj_cls.id >= min_id).order_by(old_obj_cls.id).limit(limit).all()
        while(len(old_objs) > 0):
            max_id = max([x.id for x in old_objs]) + 1
            print "{0:.2f}%".format(100.0 * (iterations + 1) * limit / old_obj_count)
            print str(min_id) + ' (' + str(create_id_f(min_id)) + ') to ' + str(max_id) + ' (' + str(create_id_f(max_id)) + ')'
            
            success = False
            while not success:
                new_session = new_session_maker()
                
                key_to_obj = cache_by_key_in_range(new_obj_cls, new_obj_cls.id, new_session, create_id_f(min_id), create_id_f(max_id))
            
                new_objs = []
                for old_obj in old_objs:
                    newly_created_objs = create_f(old_obj, precomp, **kwargs)
                    if newly_created_objs is not None:
                        new_objs.extend(newly_created_objs)
            
                success = create_or_update_and_remove(new_objs, key_to_obj, values_to_check, new_session)
            
                if ask:
                    ask_to_commit(new_session, start_time)  
                else:
                    commit_without_asking(new_session, start_time)
                new_session.close()
              
            start_time = datetime.datetime.now()  
            iterations = iterations + 1
            min_id = max_id
            old_objs = get_old_obj_query(new_session).filter(old_obj_cls.id >= min_id).order_by(old_obj_cls.id).limit(limit).all()
            
    except Exception:
        write_to_output_file( "Unexpected error:" + str(sys.exc_info()[0]) )
        raise
    finally:
        new_session.close() 
        
def execute_conversion_file(convert_f, new_session_maker, ask, **kwargs):  
    start_time = datetime.datetime.now()
    try:
        success = False
        while not success:
            new_session = new_session_maker()
            success = convert_f(new_session, **kwargs)
            if ask:
                ask_to_commit(new_session, start_time)  
            else:
                commit_without_asking(new_session, start_time)
            new_session.close()
    except Exception:
        write_to_output_file( "Unexpected error:" + str(sys.exc_info()[0]) )
        raise
    finally:
        new_session.close()  
        
def break_up_file(filename, delimeter='\t'):
    rows = []
    f = open(filename, 'r')
    for line in f:
        rows.append(line.split(delimeter))
    f.close()
    return rows

def execute_obj_conversion(new_session_maker, old_session_maker, limit, ask, 
                           grab_current_objs, grab_old_objs, convert_old_to_new, values_to_check):
    start_time = datetime.datetime.now()
    try:
        output_creator = OutputCreator()
        
        #Prepare bank of current objs
        new_session = new_session_maker()
        current_objs = grab_current_objs(new_session)
        new_session.close()
        
        key_to_current_obj = dict([(x.unique_key(), x) for x in current_objs])
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        marked_current_obj_ids = set()
        
        min_id = 0
        old_objs = None
        while old_objs is None or len(old_objs) > 0:
            #Grab old objects in batches
            old_session = old_session_maker()
            old_objs = grab_old_objs(old_session, min_id, limit)
            old_session.close()
            
            #Convert old objects to new and add or edit entries in db.
            new_session = new_session_maker()
            for old_obj in old_objs:
                new_obj = convert_old_to_new(old_obj)
                if new_obj is not None:
                    add_or_check(new_obj, key_to_current_obj, id_to_current_obj, new_obj.unique_key(), values_to_check, new_session, output_creator)
                    marked_current_obj_ids.add(new_obj.id)
                    
            #Remove unmarked objects
            
            
            #Finish and commit.
            if ask:
                ask_to_commit(new_session, start_time)  
            else:
                commit_without_asking(new_session, start_time)
            output_creator.finished()
            new_session.close()
            
            min_id = max([x.id for x in old_objs]) + 1
        
    except Exception:
        write_to_output_file( "Unexpected error:" + str(sys.exc_info()[0]) )
        raise
    finally:
        new_session.close()  

        
