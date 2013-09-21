'''
Created on Sep 20, 2013

@author: kpaskov
'''
from convert_core import create_or_update
from datetime import datetime
from mpmath import ceil
from schema_conversion import prepare_schema_connection, new_config, old_config, \
    break_up_file
from schema_conversion.output_manager import OutputCreator
import logging
import model_new_schema
import model_old_schema
import sys

#Recorded times: 

"""
--------------------- Convert Protein ---------------------
"""

def create_protein_id(old_feature_id):
    return old_feature_id + 200000

def create_protein(old_protein, id_to_bioentity):
    from model_new_schema.protein import Protein
    
    locus_id = old_protein.feature_id
    if locus_id not in id_to_bioentity:
        print 'Bioentity does not exist. ' + str(locus_id)
    locus = id_to_bioentity[locus_id]
    
    display_name = locus.display_name + 'p'
    format_name = locus.format_name = 'P'
    protein = Protein(create_protein_id(locus_id), display_name, format_name, locus_id, old_protein.length, 
                      old_protein.n_term_seq, old_protein.c_term_seq, None, old_protein.date_created, old_protein.created_by)
    return [protein]

def convert_protein(old_session_maker, new_session_maker):
    from model_new_schema.bioentity import Bioentity as NewBioentity
    from model_new_schema.protein import Protein as NewProtein
    from model_old_schema.sequence import ProteinInfo as OldProteinInfo
    
    log = logging.getLogger('convert.protein.protein')
    log.info('begin')
    output_creator = OutputCreator(log)
    
    try:
        #Grab all current objects
        new_session = new_session_maker()
        current_objs = new_session.query(NewProtein).all()
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        key_to_current_obj = dict([(x.unique_key(), x) for x in current_objs])
                
        #Values to check
        values_to_check = ['display_name', 'link', 'source', 'status', 'date_created', 'created_by',
                       'locus_id', 'length', 'n_term_seq', 'c_term_seq']
        
        untouched_obj_ids = set(id_to_current_obj.keys())
        
        #Grab old objects
        old_session = old_session_maker()
        old_objs = old_session.query(OldProteinInfo).all()
        
        #Grab cached dictionaries
        id_to_bioentity = dict([(x.id, x) for x in new_session.query(NewBioentity).all()])       
        
        for old_obj in old_objs:
            #Convert old objects into new ones
            newly_created_objs = create_protein(old_obj, id_to_bioentity)
                
            if newly_created_objs is not None:
                #Edit or add new objects
                for newly_created_obj in newly_created_objs:
                    current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                    current_obj_by_key = None if newly_created_obj.unique_key() not in key_to_current_obj else key_to_current_obj[newly_created_obj.unique_key()]
                    create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_key, values_to_check, new_session, output_creator)
                    
                    if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                        untouched_obj_ids.remove(current_obj_by_id.id)
                    if current_obj_by_key is not None and current_obj_by_key.id in untouched_obj_ids:
                        untouched_obj_ids.remove(current_obj_by_key.id)
                        
        #Delete untouched objs
        for untouched_obj_id  in untouched_obj_ids:
            new_session.delete(id_to_current_obj[untouched_obj_id])
            output_creator.removed()
        
        #Commit
        output_creator.finished()
        new_session.commit()
        
    except Exception:
        log.exception('Unexpected error:' + str(sys.exc_info()[0]))
    finally:
        new_session.close()
        old_session.close()
        
    log.info('complete')
  
"""
--------------------- Convert Domain ---------------------
"""

def create_domain(row):
    from model_new_schema.protein import Domain
    
    source = row[3]
    db_identifier = row[4]
    description = row[5]
    interpro_id = row[11]
    interpro_description = row[12]
    
    #Need to check these links
    if source == 'JASPAR':
        link = 'http://jaspar.genereg.net/cgi-bin/jaspar_db.pl?rm=present&collection=CORE&ID=' + db_identifier
    elif source == 'HMMSmart':
        source = 'SMART'
        link = 'http://smart.embl-heidelberg.de/smart/do_annotation.pl?DOMAIN=' + db_identifier
    elif source == 'HMMPfam':
        source = 'HMMPfam'
        link = 'http://pfam.sanger.ac.uk/family/' + db_identifier + '?type=Family'
    elif source == 'Gene3D':
        link = 'http://www.cathdb.info/version/latest/superfamily/' + db_identifier[6:]
    elif source == 'superfamily':
        source = 'SUPERFAMILY'
        link = 'http://supfam.org/SUPERFAMILY/cgi-bin/scop.cgi?ipid=' + db_identifier
    elif source == 'Seg':
        return None
    elif source == 'Coil':
        return None
    elif source == 'HMMPanther':
        source = 'Panther'
        link = 'http://www.pantherdb.org/panther/family.do?clsAccession=' + db_identifier
    elif source == 'HMMTigr':
        link = 'http://www.jcvi.org/cgi-bin/tigrfams/HmmReportPage.cgi?acc=' + db_identifier
    elif source == 'FPrintScan':
        source = 'PRINTS'
        link = 'http://www.bioinf.man.ac.uk/cgi-bin/dbbrowser/sprint/searchprintss.cgi?display_opts=Prints&category=None&queryform=false&prints_accn=' + db_identifier
    elif source == 'BlastProDom':
        link = 'http://prodom.prabi.fr/prodom/current/cgi-bin/request.pl?question=DBEN&query=' + db_identifier
    elif source == 'HMMPIR':
        link = 'http://pir.georgetown.edu/cgi-bin/ipcSF?id=' + db_identifier
    elif source == 'ProfileScan':
        link = 'http://prosite.expasy.org/' + db_identifier
    else:
        print 'No link for source = ' + source + ' ' + str(db_identifier)
        return None
    
    domain = Domain(source, db_identifier, description, interpro_id, interpro_description, link)
    return [domain]

def create_domain_from_tf_file(row):
    from model_new_schema.protein import Domain
    
    source = 'JASPAR'
    db_identifier = row[0]
    description = row[3]
    interpro_id = None
    interpro_description = None
    
    link = 'http://jaspar.genereg.net/cgi-bin/jaspar_db.pl?rm=present&collection=CORE&ID=' + db_identifier
    
    domain = Domain(source, db_identifier, description, interpro_id, interpro_description, link)
    return [domain]

def convert_domain(new_session_maker, chunk_size):
    from model_new_schema.protein import Domain as Domain
    
    log = logging.getLogger('convert.protein.domain')
    log.info('begin')
    output_creator = OutputCreator(log)
    
    try:
        #Grab all current objects
        new_session = new_session_maker()
        current_objs = new_session.query(Domain).all()
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        key_to_current_obj = dict([(x.unique_key(), x) for x in current_objs])
                
        #Values to check
        values_to_check = ['description', 'interpro_id', 'interpro_description', 'link']
        
        untouched_obj_ids = set(id_to_current_obj.keys())
        
        #Grab old objects
        data = break_up_file('/Users/kpaskov/final/domains.tab.tab')
        
        used_unique_keys = set()   
        
        min_id = 0
        count = len(data)
        num_chunks = ceil(1.0*count/chunk_size)
        for i in range(0, num_chunks):
            old_objs = data[min_id:min_id+chunk_size]
            for old_obj in old_objs:
                #Convert old objects into new ones
                newly_created_objs = create_domain(old_obj)
                    
                if newly_created_objs is not None:
                    #Edit or add new objects
                    for newly_created_obj in newly_created_objs:
                        unique_key = newly_created_obj.unique_key()
                        if unique_key not in used_unique_keys:
                            current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                            current_obj_by_key = None if unique_key not in key_to_current_obj else key_to_current_obj[unique_key]
                            create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_key, values_to_check, new_session, output_creator)
                            used_unique_keys.add(unique_key)
                            
                        if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                            untouched_obj_ids.remove(current_obj_by_id.id)
                        if current_obj_by_key is not None and current_obj_by_key.id in untouched_obj_ids:
                            untouched_obj_ids.remove(current_obj_by_key.id)
                            
            output_creator.finished(str(i+1) + "/" + str(int(num_chunks)))
            new_session.commit()
            min_id = min_id+chunk_size
            
        #Grab JASPAR domains from file
        old_objs = break_up_file('/Users/kpaskov/final/TF_family_class_accession04302013.txt')
        for old_obj in old_objs:
            #Convert old objects into new ones
            newly_created_objs = create_domain_from_tf_file(old_obj)
                
            if newly_created_objs is not None:
                #Edit or add new objects
                for newly_created_obj in newly_created_objs:
                    unique_key = newly_created_obj.unique_key()
                    if unique_key not in used_unique_keys:
                        current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                        current_obj_by_key = None if unique_key not in key_to_current_obj else key_to_current_obj[unique_key]
                        create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_key, values_to_check, new_session, output_creator)
                        used_unique_keys.add(unique_key)
                        
                    if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                        untouched_obj_ids.remove(current_obj_by_id.id)
                    if current_obj_by_key is not None and current_obj_by_key.id in untouched_obj_ids:
                        untouched_obj_ids.remove(current_obj_by_key.id)
                        
        output_creator.finished("1/1")
        new_session.commit()
                        
        #Delete untouched objs
        for untouched_obj_id  in untouched_obj_ids:
            new_session.delete(id_to_current_obj[untouched_obj_id])
            output_creator.removed()
        
        #Commit
        output_creator.finished()
        new_session.commit()
        
    except Exception:
        log.exception('Unexpected error:' + str(sys.exc_info()[0]))
    finally:
        new_session.close()
        
    log.info('complete')
 
"""
--------------------- Convert Domain Evidence ---------------------
"""

def create_domain_evidence_id(row_id):
    return row_id + 80000000

def create_domain_evidence(row, row_id, key_to_bioentity, key_to_domain):
    from model_new_schema.protein import Domainevidence
    
    bioent_format_name = row[0]
    source = row[3]
    db_identifier = row[4]
    start = int(row[6])
    end = int(row[7])
    evalue = row[8]
    status = row[9]
    date_of_run = row[10]
    
    bioent_key = (bioent_format_name + 'p', 'PROTEIN')
    if bioent_key not in key_to_bioentity:
        print 'Protein not found. ' + bioent_format_name + 'p'
        return None
    protein_id = key_to_bioentity[bioent_key].id
    
    domain_key = (db_identifier, source)
    if domain_key not in key_to_domain:
        return None
    domain_id = key_to_domain[domain_key].id
    
    #S288C
    strain_id = 1
    
    domain_evidence = Domainevidence(create_domain_evidence_id(row_id), None, strain_id, source, 
                                     start, end, evalue, status, date_of_run, protein_id, domain_id, None, None)
    return [domain_evidence]

def create_domain_evidence_from_tf_file(row, row_id, key_to_bioentity, key_to_domain, pubmed_id_to_reference_id):
    from model_new_schema.protein import Domainevidence
    
    bioent_format_name = row[2]
    source = 'JASPAR'
    db_identifier = row[0]
    start = 1
    evalue = None
    status = 'T'
    date_of_run = None
    pubmed_id = row[6]
    
    bioent_key = (bioent_format_name + 'p', 'PROTEIN')
    if bioent_key not in key_to_bioentity:
        print 'Protein not found. ' + bioent_format_name + 'p'
        return None
    protein = key_to_bioentity[bioent_key]
    protein_id = protein.id
    end = protein.length
    
    reference_id = None
    if pubmed_id in pubmed_id_to_reference_id:
        reference_id = pubmed_id_to_reference_id[pubmed_id]
    
    domain_key = (db_identifier, source)
    if domain_key not in key_to_domain:
        print 'Domain not found. ' + str(domain_key)
    domain_id = key_to_domain[domain_key].id
    
    #S288C
    strain_id = 1
    
    domain_evidence = Domainevidence(create_domain_evidence_id(row_id), reference_id, strain_id, source, 
                                     start, end, evalue, status, date_of_run, protein_id, domain_id, None, None)
    return [domain_evidence]

def convert_domain_evidence(new_session_maker, chunk_size):
    from model_new_schema.protein import Domain, Domainevidence
    from model_new_schema.bioentity import Bioentity
    from model_new_schema.reference import Reference
    
    log = logging.getLogger('convert.protein.domain_evidence')
    log.info('begin')
    output_creator = OutputCreator(log)
    
    try:
        #Grab all current objects
        new_session = new_session_maker()
        current_objs = new_session.query(Domainevidence).all()
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        key_to_current_obj = dict([(x.unique_key(), x) for x in current_objs])
                
        #Values to check
        values_to_check = ['reference_id', 'strain_id', 'source', 'date_created', 'created_by',
                           'start', 'end', 'evalue', 'status', 'date_of_run', 'protein_id', 'domain_id']
        
        #Grab cached dictionaries
        key_to_bioentity = dict([(x.unique_key(), x) for x in new_session.query(Bioentity).all()])       
        key_to_domain = dict([(x.unique_key(), x) for x in new_session.query(Domain).all()]) 
        pubmed_id_to_reference_id = dict([(x.pubmed_id, x.id) for x in new_session.query(Reference).all()]) 
        
        untouched_obj_ids = set(id_to_current_obj.keys())
        
        #Grab old objects
        data = break_up_file('/Users/kpaskov/final/domains.tab.tab')
        
        used_unique_keys = set()   
        
        j=0
        min_id = 0
        count = len(data)
        num_chunks = ceil(1.0*count/chunk_size)
        for i in range(0, num_chunks):
            old_objs = data[min_id:min_id+chunk_size]
            for old_obj in old_objs:
                #Convert old objects into new ones
                newly_created_objs = create_domain_evidence(old_obj, j, key_to_bioentity, key_to_domain)
                    
                if newly_created_objs is not None:
                    #Edit or add new objects
                    for newly_created_obj in newly_created_objs:
                        unique_key = newly_created_obj.unique_key()
                        if unique_key not in used_unique_keys:
                            current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                            current_obj_by_key = None if unique_key not in key_to_current_obj else key_to_current_obj[unique_key]
                            create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_key, values_to_check, new_session, output_creator)
                            used_unique_keys.add(unique_key)
                            
                        if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                            untouched_obj_ids.remove(current_obj_by_id.id)
                        if current_obj_by_key is not None and current_obj_by_key.id in untouched_obj_ids:
                            untouched_obj_ids.remove(current_obj_by_key.id)
                j = j+1
                            
            output_creator.finished(str(i+1) + "/" + str(int(num_chunks)))
            new_session.commit()
            min_id = min_id+chunk_size
            
        #Grab JASPAR evidence from file
        old_objs = break_up_file('/Users/kpaskov/final/TF_family_class_accession04302013.txt')
        for old_obj in old_objs:
            #Convert old objects into new ones
            newly_created_objs = create_domain_evidence_from_tf_file(old_obj, j, key_to_bioentity, key_to_domain, pubmed_id_to_reference_id)
                
            if newly_created_objs is not None:
                #Edit or add new objects
                for newly_created_obj in newly_created_objs:
                    unique_key = newly_created_obj.unique_key()
                    if unique_key not in used_unique_keys:
                        current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                        current_obj_by_key = None if unique_key not in key_to_current_obj else key_to_current_obj[unique_key]
                        create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_key, values_to_check, new_session, output_creator)
                        used_unique_keys.add(unique_key)
                        
                    if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                        untouched_obj_ids.remove(current_obj_by_id.id)
                    if current_obj_by_key is not None and current_obj_by_key.id in untouched_obj_ids:
                        untouched_obj_ids.remove(current_obj_by_key.id)
            j = j+1
                        
        output_creator.finished("1/1")
        new_session.commit()
                        
        #Delete untouched objs
        for untouched_obj_id  in untouched_obj_ids:
            new_session.delete(id_to_current_obj[untouched_obj_id])
            output_creator.removed()
        
        #Commit
        output_creator.finished()
        new_session.commit()
        
    except Exception:
        log.exception('Unexpected error:' + str(sys.exc_info()[0]))
    finally:
        new_session.close()
        
    log.info('complete')
 
"""
---------------------Convert------------------------------
"""   

def convert(old_session_maker, new_session_maker):  
    logging.basicConfig(format='%(asctime)s %(name)s: %(message)s', level=logging.DEBUG, datefmt='%m/%d/%Y %H:%M:%S')
    
    log = logging.getLogger('convert.protein')
    
    hdlr = logging.FileHandler('/Users/kpaskov/Documents/Schema Conversion Logs/convert.protein.' + str(datetime.now()) + '.txt')
    formatter = logging.Formatter('%(asctime)s %(name)s: %(message)s', '%m/%d/%Y %H:%M:%S')
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr) 
    log.setLevel(logging.DEBUG)
    
    log.info('begin')
        
    convert_protein(old_session_maker, new_session_maker)
    
    #convert_domain(new_session_maker, 5000)
    
    convert_domain_evidence(new_session_maker, 5000)
    
    log.info('complete')
    
if __name__ == "__main__":
    old_session_maker = prepare_schema_connection(model_old_schema, old_config)
    new_session_maker = prepare_schema_connection(model_new_schema, new_config)
    convert(old_session_maker, new_session_maker)   
    
    