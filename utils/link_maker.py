'''
Created on Mar 6, 2013

@author: kpaskov
'''

#Bioconcept links
def biocon_link(biocon_type, format_name):
    return '/%s/%s' % (biocon_type, format_name)
    
#Bioentity links
def bioent_link(bioent_type, format_name):
    return '/%s/%s' % (bioent_type, format_name)

#Chemical links
def chemical_link(format_name):
    return '/chemical/' +format_name

#Reference links
def reference_link(format_name):
    return '/reference/' + format_name

#Author links
def author_link(format_name):
    return '/author/' + format_name

#Experiment links
def experiment_link(format_name):
    return '/experiment/' + format_name

#Strain links
def strain_link(format_name):
    return '/strain/' + format_name
