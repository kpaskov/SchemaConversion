'''
Created on May 28, 2013

@author: kpaskov
'''
from schema_conversion import create_or_update_and_remove, cache_by_key, \
    cache_by_id, cache_by_key_in_range, cache_ids_in_range, cache_by_id_in_range

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

def create_interaction_format_name(bioent1, bioent2):
    if bioent1.id < bioent2.id:
        return bioent1.format_name + '__' + bioent2.format_name
    else:
        return bioent2.format_name + '__' + bioent1.format_name
    
def create_interaction_id(evidence_id):
    return evidence_id

def create_interaction_family_id(interaction_id):
    return interaction_id

def create_interaction(evidence, format_name_to_info, id_to_bioent):
    from model_new_schema.auxiliary import Interaction
    bioent1_id = evidence.bioentity1_id
    bioent2_id = evidence.bioentity2_id
    bioent1 = id_to_bioent[bioent1_id]
    bioent2 = id_to_bioent[bioent2_id]
    format_name = create_interaction_format_name(bioent1, bioent2)
    evidence_count, min_id = format_name_to_info[format_name]
    if min_id == create_interaction_id(evidence.id):
        interaction = Interaction(create_interaction_id(evidence.id), evidence.class_type, format_name, format_name, bioent1_id, bioent2_id)
        interaction.evidence_count = evidence_count
        return [interaction]
    return None

def create_interaction_precomp(evidences, id_to_bioent):
    format_name_to_evidence_count = {}
    for evidence in evidences:
        bioent1_id = evidence.bioentity1_id
        bioent2_id = evidence.bioentity2_id
        bioent1 = id_to_bioent[bioent1_id]
        bioent2 = id_to_bioent[bioent2_id]
        format_name = create_interaction_format_name(bioent1, bioent2)
        if format_name in format_name_to_evidence_count:
            evidence_count, min_id = format_name_to_evidence_count[format_name]
            format_name_to_evidence_count[format_name] = (evidence_count + 1, min(min_id, create_interaction_id(evidence.id)))
        else:
            format_name_to_evidence_count[format_name] = (1, create_interaction_id(evidence.id))
    return format_name_to_evidence_count

def create_interaction_family(interaction, precomp_info, max_neighbors, id_to_bioent):
    from model_new_schema.auxiliary import InteractionFamily as NewInteractionFamily

    bioent_id_to_evidence_cutoff, bioent_id_to_neighbor_ids, edge_to_counts = precomp_info
    
    interaction_type = interaction.class_type
    bioent1_id, bioent2_id = order_bioent_ids(interaction.bioentity1_id, interaction.bioentity2_id)
    key = (bioent1_id, bioent2_id)
    edge_counts = edge_to_counts[key]
    phys_count = 0 if 'PHYSINTERACTION' not in edge_counts else edge_counts['PHYSINTERACTION']
    gen_count = 0 if 'GENINTERACTION' not in edge_counts else edge_counts['GENINTERACTION']
    total_count = sum(edge_counts.values())
    is_first_interaction_type = sorted(edge_to_counts[key].keys())[0] == interaction_type
    
    if not is_first_interaction_type:
        return None
    
    interaction_families = []
    
    #Check endpoint1
    if bioent_id_to_evidence_cutoff[bioent1_id] <= total_count:    
        interaction_families.append(NewInteractionFamily(create_interaction_family_id(interaction.id), bioent1_id, bioent1_id, bioent2_id, gen_count, phys_count, total_count))
    
    #Check endpoint2
    if bioent_id_to_evidence_cutoff[bioent2_id] <= total_count:    
        interaction_families.append(NewInteractionFamily(create_interaction_family_id(interaction.id), bioent2_id, bioent1_id, bioent2_id, gen_count, phys_count, total_count))
    
    #Check overlap
    bioent1_neighbors = bioent_id_to_neighbor_ids[bioent1_id]
    bioent2_neighbors = bioent_id_to_neighbor_ids[bioent2_id]
    overlap = bioent1_neighbors & bioent2_neighbors
    for bioent_id in overlap:
        if bioent_id_to_evidence_cutoff[bioent_id] <= total_count: 
            interaction_families.append(NewInteractionFamily(create_interaction_family_id(interaction.id), bioent_id, bioent1_id, bioent2_id, gen_count, phys_count, total_count))
    return interaction_families
    

def create_interaction_family_precomp(interactions, max_neighbors, id_to_bioent):
    bioent_id_to_neighbor_ids = {}
    edge_to_counts = {}
    
    # Build a set of neighbors for every bioent.
    for bioent_id in id_to_bioent.keys():
        bioent_id_to_neighbor_ids[bioent_id] = set()
        
    for interaction in interactions:
        bioent1_id = interaction.bioentity1_id
        bioent2_id = interaction.bioentity2_id
        if bioent2_id not in bioent_id_to_neighbor_ids[bioent1_id]:
            bioent_id_to_neighbor_ids[bioent1_id].add(bioent2_id)
        if bioent1_id not in bioent_id_to_neighbor_ids[bioent2_id]:
            bioent_id_to_neighbor_ids[bioent2_id].add(bioent1_id)
                
    # Build a set of maximum counts for each interaction_type for each edge
    for interaction in interactions:
        interaction_type = interaction.class_type
        bioent1_id, bioent2_id = order_bioent_ids(interaction.bioentity1_id, interaction.bioentity2_id)
        key = (bioent1_id, bioent2_id)
            
        if key not in edge_to_counts:
            edge_to_counts[key]  = {interaction_type: interaction.evidence_count}
        elif interaction_type in edge_to_counts[key]:
            edge_to_counts[key][interaction_type] = edge_to_counts[key][interaction_type] + interaction.evidence_count
        else:
            edge_to_counts[key][interaction_type] = interaction.evidence_count
                      
    bioent_id_to_evidence_cutoff = {}
          
    for bioent_id in id_to_bioent.keys():
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
            
        bioent_id_to_evidence_cutoff[bioent_id] = min_evidence_count
        
    return (bioent_id_to_evidence_cutoff, bioent_id_to_neighbor_ids, edge_to_counts)
        

def order_bioent_ids(bioent1_id, bioent2_id):
    if bioent1_id < bioent2_id:
        return bioent1_id, bioent2_id
    else:
        return bioent2_id, bioent1_id
    
def convert_biofact(new_session, biocon_type, key_to_evidence, key_to_bioconrels, min_id, max_id):
    from model_new_schema.auxiliary import Biofact as NewBiofact
    
    key_to_biofacts = cache_by_key_in_range(NewBiofact, NewBiofact.bioconcept_id, new_session, min_id, max_id, biocon_type=biocon_type)
    
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
    from model_new_schema.bioconcept import BioconceptRelation as NewBioconceptRelation
    from model_new_schema.auxiliary import BioconceptAncestor as NewBioconceptAncestor
    
    #Cache biocon_relations and biocon_ancestors
    key_to_biocon_relations = cache_by_key(NewBioconceptRelation, new_session, bioconrel_type=bioconrel_type)
    
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
        key_to_biocon_ancestors = cache_by_key(NewBioconceptAncestor, new_session, bioconanc_type=bioconrel_type, generation=generation)
        new_biocon_ancestors = []    

        for child_id, all_ancestor_ids in child_to_ancestors.iteritems():
            this_generation = all_ancestor_ids[generation-1]
            new_biocon_ancestors.extend([NewBioconceptAncestor(ancestor_id, child_id, bioconrel_type, generation) for ancestor_id in this_generation])
        create_or_update_and_remove(new_biocon_ancestors, key_to_biocon_ancestors, [], new_session) 
    return True

def create_bioent_reference_id(evidence_id):
    return evidence_id

def create_bioent_reference(evidence, bioent_f):
    from model_new_schema.auxiliary import BioentityReference as NewBioentityReference
    reference_id = evidence.reference_id
    bioent_ids = bioent_f(evidence)
    for bioent_id in bioent_ids:
        if reference_id is not None and bioent_id is not None and (bioent_id, reference_id):
            return NewBioentityReference(evidence.class_type, bioent_id, reference_id)
    return None
    
    
    