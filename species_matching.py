from fuzzywuzzy import process
import numpy as np
import pandas as pd


def clean_species_str(species_str):
    black_lst = [
        "upland",
        "",
        "lowland",
        "tropical hardwoods",  # too vague to map, and unused...
        "mixed upland hardwoods",
    ]
    species_lst = species_str.split(",")
    return [
        species.strip().lower() for species in species_lst if species not in black_lst
    ]


def species_lst_to_arr(lst, n_species=999):
    """
    cast sparse list of species FIA numbers to array for cosine comparison
    pretty sure all NA species are < 999
    """
    arr = np.zeros(n_species)
    arr[lst] = 1
    return arr


def encode_species_lst(spp_lst, fia_species_map):
    return [fia_species_map[spp] for spp in spp_lst if spp in fia_species_map]


def get_species_arr(species_str, fia_species_map):
    """
    Translate ARB assessment area species string to (999,) vector for similarity scoring
    """
    species_lst = clean_species_str(species_str)
    encoded_lst = encode_species_lst(species_lst, fia_species_map)
    species_arr = species_lst_to_arr(encoded_lst)
    return species_arr


def load_fia_species_map(use_cache=True):

    if use_cache:
        out_fname = "data/fia_species_map.json"
        try:
            data = json.load(open(out_fname))
            return data
        except:
            data = generate_assessment_fia_species_map()
            return data

    return generate_assessment_fia_species_map()

