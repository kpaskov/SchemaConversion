'''
Created on May 28, 2013

@author: kpaskov
'''
from schema_conversion import create_or_update_and_remove, cache_by_key, \
    cache_by_id, cache_by_key_in_range, cache_ids_in_range, cache_by_id_in_range
from sqlalchemy.orm import joinedload

def update_biocon_gene_counts(new_session, biocon_cls, evidence_cls):
    '''
    Update goterm gene counts
    '''

    biocons = new_session.query(biocon_cls).all()
    evidences = new_session.query(evidence_cls).all()
    biocon_id_to_bioent_ids = {}
    
    for biocon in biocons:
        biocon_id_to_bioent_ids[biocon.id] = set()
        
    for evidence in evidences:
        biocon_id_to_bioent_ids[evidence.biocon_id].add(evidence.bioent_id)
        
    num_changed = 0
    for biocon in biocons:
        count = len(biocon_id_to_bioent_ids[biocon.id])
        if count != biocon.direct_gene_count:
            biocon.direct_gene_count = count
            num_changed = num_changed + 1
    print 'In total ' + str(num_changed) + ' changed.'
    return True

def create_reference_bibs(new_session, min_id, max_id):
    from model_new_schema.reference import Reference, ReferenceBib
    
    key_to_refbib = cache_by_key_in_range(ReferenceBib, ReferenceBib.id, new_session, min_id, max_id)
    references = cache_by_id_in_range(Reference, Reference.id, new_session, min_id, max_id).values()
    
    new_refbibs = [create_ref_bib(reference) for reference in references]
    
    values_to_check = ['bib_entry']
    success = create_or_update_and_remove(new_refbibs, key_to_refbib, values_to_check, new_session)
    return success   

def create_ref_bib(reference):
    from model_new_schema.reference import ReferenceBib
    entries = []
    entries.append('PMID- ' + str(reference.pubmed_id)) 
    entries.append('STAT- ' + str(reference.status))
    entries.append('DP  - ' + str(reference.date_published)) 
    entries.append('TI  - ' + str(reference.title))
    entries.append('SO  - ' + str(reference.source)) 
    entries.append('LR  - ' + str(reference.date_revised)) 
    entries.append('IP  - ' + str(reference.issue)) 
    entries.append('PG  - ' + str(reference.page)) 
    entries.append('VI  - ' + str(reference.volume)) 
        
    for author in reference.author_names:
        entries.append('AU  - ' + author)
    for reftype in reference.reftype_names:
        entries.append('PT  - ' + reftype)
        
    if reference.abstract_obj is not None:
        entries.append('AB  - ' + reference.abstract)
        
    if reference.journal is not None:
        entries.append('TA  - ' + str(reference.journal.abbreviation)) 
        entries.append('JT  - ' + str(reference.journal.full_name)) 
        entries.append('IS  - ' + str(reference.journal.issn)) 

        
    if reference.book is not None:
        entries.append('PL  - ' + str(reference.book.publisher_location)) 
        entries.append('BTI - ' + str(reference.book.title))
        entries.append('VTI - ' + str(reference.book.volume_title)) 
        entries.append('ISBN- ' + str(reference.book.isbn))     
    ref_bib = ReferenceBib(reference.id, '\n'.join(entries))
    return ref_bib
    
    

def create_interaction_format_name(bioent1, bioent2):
    if bioent1.id < bioent2.id:
        return bioent1.format_name + '__' + bioent2.format_name
    else:
        return bioent2.format_name + '__' + bioent1.format_name

def convert_interactions(new_session, interaction_type, evidence_cls):
    #Cache interactions
    from model_new_schema.auxiliary import Interaction as NewInteraction
    from model_new_schema.bioentity import Bioentity as NewBioentity
    key_to_interactions = cache_by_key(NewInteraction, new_session, interaction_type=interaction_type)
    key_to_evidence = cache_by_key(evidence_cls, new_session)
    id_to_bioent = cache_by_id(NewBioentity, new_session)
    
    new_interactions = []
    format_name_to_evidence_count = {}
    for evidence in key_to_evidence.values():
        bioent1_id = evidence.bioent1_id
        bioent2_id = evidence.bioent2_id
        bioent1 = id_to_bioent[bioent1_id]
        bioent2 = id_to_bioent[bioent2_id]
        format_name = create_interaction_format_name(bioent1, bioent2)
        if format_name in format_name_to_evidence_count:
            format_name_to_evidence_count[format_name] = format_name_to_evidence_count[format_name] + 1
        else:
            format_name_to_evidence_count[format_name] = 1
        interaction = NewInteraction(evidence.id, interaction_type, format_name, format_name, bioent1_id, bioent2_id)
        new_interactions.append(interaction)
        
    for interaction in new_interactions:
        interaction.evidence_count = format_name_to_evidence_count[interaction.format_name]
        
    values_to_check = ['display_name', 'bioent1_id', 'bioent2_id', 'evidence_count']
    success = create_or_update_and_remove(new_interactions, key_to_interactions, values_to_check, new_session)
    return success          
            
def convert_interaction_families(new_session, interaction_types, max_neighbors, min_id, max_id):
    from model_new_schema.auxiliary import Interaction as NewInteraction, InteractionFamily as NewInteractionFamily
    from model_new_schema.bioentity import Bioentity as NewBioentity
    
    id_to_bioent = cache_by_id(NewBioentity, new_session)
    range_bioent_ids = cache_ids_in_range(NewBioentity, NewBioentity.id, new_session, min_id, max_id)
    key_to_interfams = cache_by_key_in_range(NewInteractionFamily, NewInteractionFamily.bioent_id, new_session, min_id, max_id)
    
    
    bioent_id_to_neighbor_ids = {}
    edge_to_counts = {}
    
    all_interactions = dict()
    for interaction_type in interaction_types:
        all_interactions[interaction_type] = cache_by_id(NewInteraction, new_session, interaction_type=interaction_type)
    
    # Build a set of neighbors for every bioent.
    for bioent_id in id_to_bioent.keys():
        bioent_id_to_neighbor_ids[bioent_id] = set()
        
    for interaction_type in interaction_types:
        id_to_interaction = all_interactions[interaction_type]
        for interaction in id_to_interaction.values():
            bioent1_id = interaction.bioent1_id
            bioent2_id = interaction.bioent2_id
            if bioent2_id not in bioent_id_to_neighbor_ids[bioent1_id]:
                bioent_id_to_neighbor_ids[bioent1_id].add(bioent2_id)
            if bioent1_id not in bioent_id_to_neighbor_ids[bioent2_id]:
                bioent_id_to_neighbor_ids[bioent2_id].add(bioent1_id)
                
    # Build a set of maximum counts (for each interaction_type and total) for each edge
    for interaction_type in interaction_types:
        id_to_interaction = all_interactions[interaction_type]
        for interaction in id_to_interaction.values():
            bioent1_id, bioent2_id = order_bioent_ids(interaction.bioent1_id, interaction.bioent2_id)
            key = (bioent1_id, bioent2_id)
            
            if key not in edge_to_counts:
                edge_to_counts[key]  = {interaction_type: interaction.evidence_count}
            elif interaction_type in edge_to_counts[key]:
                edge_to_counts[key][interaction_type] = edge_to_counts[key][interaction_type] + interaction.evidence_count
            else:
                edge_to_counts[key][interaction_type] = interaction.evidence_count
                
    interfams = []
                
    for bioent_id in range_bioent_ids:
        neighbor_ids = bioent_id_to_neighbor_ids[bioent_id]
        
        # Calculate evidence cutoffs.
        evidence_cutoffs = [0, 0, 0, 0]
        for neighbor_id in neighbor_ids:
            neigh_ev_count = sum(edge_to_counts[order_bioent_ids(bioent_id, neighbor_id)].values())
            index = min(neigh_ev_count, 3)
            evidence_cutoffs[index] = evidence_cutoffs[index]+1
          
        if evidence_cutoffs[2] + evidence_cutoffs[3] > max_neighbors:
            min_evidence_count = 3
        elif evidence_cutoffs[1] + evidence_cutoffs[2] + evidence_cutoffs[3] > max_neighbors:
            min_evidence_count = 2
        else:
            min_evidence_count = 1 
            
        # Build a "Star Graph" with the bioent in the center and all neighbors with evidence above the cutoff connected to this
        # center node.
        cutoff_neighbor_ids = [neighbor_id for neighbor_id in neighbor_ids if sum(edge_to_counts[order_bioent_ids(bioent_id, neighbor_id)].values()) >= min_evidence_count]
        for neighbor_id in cutoff_neighbor_ids:
            bioent1_id, bioent2_id = order_bioent_ids(bioent_id, neighbor_id)
            bioent1 = id_to_bioent[bioent1_id]
            bioent2 = id_to_bioent[bioent2_id]
            neigh_ev_counts = edge_to_counts[order_bioent_ids(bioent_id, neighbor_id)]
            interfams.append(create_interaction_family(bioent_id, bioent1, bioent2, neigh_ev_counts))
                
        # Now add edges connecting nodes in the star.
        for neighbor_id in cutoff_neighbor_ids:
            for neigh_of_neigh_id in bioent_id_to_neighbor_ids[neighbor_id]:
                neigh_of_neigh_ev_counts = edge_to_counts[order_bioent_ids(neighbor_id, neigh_of_neigh_id)]
                neigh_of_neigh_ev_count = sum(neigh_of_neigh_ev_counts.values())
                if neigh_of_neigh_ev_count >= min_evidence_count and neigh_of_neigh_id in cutoff_neighbor_ids:
                    bioent1_id, bioent2_id = order_bioent_ids(neighbor_id, neigh_of_neigh_id)
                    bioent1 = id_to_bioent[bioent1_id]
                    bioent2 = id_to_bioent[bioent2_id]
                    interfams.append(create_interaction_family(bioent_id, bioent1, bioent2, neigh_of_neigh_ev_counts))
        
    values_to_check = ['genetic_ev_count', 'physical_ev_count', 'evidence_count']
    success = create_or_update_and_remove(interfams, key_to_interfams, values_to_check, new_session)
    return success 

def create_interaction_family(bioent_id, bioent1, bioent2, evidence_counts):
    from model_new_schema.auxiliary import InteractionFamily as NewInteractionFamily
    
    phys_count = 0 if 'PHYSICAL_INTERACTION' not in evidence_counts else evidence_counts['PHYSICAL_INTERACTION']
    gen_count = 0 if 'GENETIC_INTERACTION' not in evidence_counts else evidence_counts['GENETIC_INTERACTION']
    total_count = sum(evidence_counts.values())
                
    return NewInteractionFamily(bioent_id, bioent1.id, bioent2.id, gen_count, phys_count, total_count)


def order_bioent_ids(bioent1_id, bioent2_id):
    if bioent1_id < bioent2_id:
        return bioent1_id, bioent2_id
    else:
        return bioent2_id, bioent1_id
    
def convert_biofact(new_session, biocon_type, key_to_evidence, key_to_bioconrels, min_id, max_id):
    from model_new_schema.auxiliary import Biofact as NewBiofact
    
    key_to_biofacts = cache_by_key_in_range(NewBiofact, NewBiofact.biocon_id, new_session, min_id, max_id, biocon_type=biocon_type)
    
    child_to_parents = {}
    for bioconrel in key_to_bioconrels.values():
        child_id = bioconrel.child_id
        parent_id = bioconrel.parent_id
        
        if child_id in child_to_parents:
            child_to_parents[child_id].add(parent_id)
        else:
            child_to_parents[child_id] = set([parent_id])
    
    new_biofacts = set()
    for evidence in key_to_evidence.values():
        biocon_ids = [evidence.biocon_id]
        next_gen_biocon_ids = set()
        while len(biocon_ids) > 0:
            for biocon_id in biocon_ids:
                if biocon_id >= min_id and biocon_id < max_id:
                    new_biofacts.add(NewBiofact(evidence.bioent_id, biocon_id, biocon_type))
                if biocon_id in child_to_parents:
                    next_gen_biocon_ids.update(child_to_parents[biocon_id])
            biocon_ids = set(next_gen_biocon_ids)
            next_gen_biocon_ids = set()
    success = create_or_update_and_remove(new_biofacts, key_to_biofacts, [], new_session)
    return success

def convert_biocon_ancestors(new_session, bioconrel_type, num_generations):
    from model_new_schema.bioconcept import BioconRelation as NewBioconRelation
    from model_new_schema.auxiliary import BioconAncestor as NewBioconAncestor
    
    #Cache biocon_relations and biocon_ancestors
    key_to_biocon_relations = cache_by_key(NewBioconRelation, new_session, bioconrel_type=bioconrel_type)
    
    child_to_parents = {}
    for biocon_relation in key_to_biocon_relations.values():
        child_id = biocon_relation.child_id
        parent_id = biocon_relation.parent_id
        if child_id not in child_to_parents:
            child_to_parents[child_id] = set()
        if parent_id not in child_to_parents:
            child_to_parents[parent_id] = set()
            
        child_to_parents[child_id].add(parent_id)

    child_to_ancestors = dict([(child_id, [parent_ids]) for child_id, parent_ids in child_to_parents.iteritems()])
    for i in range(2, num_generations):
        for ancestor_ids in child_to_ancestors.values():
            last_generation = ancestor_ids[-1]
            new_generation = set()
            [new_generation.update(child_to_parents[child_id]) for child_id in last_generation]
            ancestor_ids.append(new_generation)
        
    
    for generation in range(1, num_generations):
        print 'Generation ' + str(generation)
        key_to_biocon_ancestors = cache_by_key(NewBioconAncestor, new_session, bioconanc_type=bioconrel_type, generation=generation)
        new_biocon_ancestors = []    

        for child_id, all_ancestor_ids in child_to_ancestors.iteritems():
            this_generation = all_ancestor_ids[generation-1]
            new_biocon_ancestors.extend([NewBioconAncestor(ancestor_id, child_id, bioconrel_type, generation) for ancestor_id in this_generation])
        create_or_update_and_remove(new_biocon_ancestors, key_to_biocon_ancestors, [], new_session) 
    return True
        
def convert_bioent_references(new_session, evidences, bioent_ref_type, bioent_f, min_id, max_id):
    
    from model_new_schema.auxiliary import BioentReference as NewBioentReference
    
    key_to_bioent_reference = cache_by_key_in_range(NewBioentReference, NewBioentReference.bioent_id, new_session, min_id, max_id, bioent_ref_type=bioent_ref_type)
    
    new_bioent_refs = {}
    for evidence in evidences:
        reference_id = evidence.reference_id
        bioent_ids = bioent_f(evidence)
        for bioent_id in bioent_ids:
            if reference_id is not None and bioent_id is not None and (bioent_id, reference_id) not in new_bioent_refs and bioent_id > min_id and bioent_id <= max_id:
                new_bioent_ref = NewBioentReference(bioent_ref_type, bioent_id, reference_id)
                new_bioent_refs[(bioent_id, reference_id)] = new_bioent_ref
            
    values_to_check = []
    success = create_or_update_and_remove(new_bioent_refs.values(), key_to_bioent_reference, values_to_check, new_session)
    return success 
    
    
    