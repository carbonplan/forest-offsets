import sys

sys.path.append("~/proj/carbonplan/retro")

from rectify_arb_assessment_areas import load_aa
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

import pandas as pd
import json

from fuzzywuzzy import process

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


def load_assessment_fia_map(use_cache=True):
    """

    Loosely adapted from https://stackoverflow.com/questions/13636848/is-it-possible-to-do-fuzzy-match-merge-with-python-pandas
    """
    out_fname = "data/fia_species_name_map.json"
    if use_cache:
        try:
            data = json.load(out_fname)
            print(f"loading fia map from {out_fname}")
        except:
            print(f"loading from cache failed -- rerunning")
            data = generate_assessment_fia_species_map()
            with open(out_fname, "w") as f:
                json.dump(data, f)
    else:
        data = generate_assessment_fia_species_map()

    return data


def generate_assessment_fia_species_map():
    fia = pd.read_excel(
        "/Users/darryl/proj/carbonplan/retro/data/FHM_Species_codes.xls", skiprows=1
    )
    aa_luts = load_aa()
    full_species_lst = []
    for aa_lut in aa_luts.values():

        aa_species_lst = (
            (aa_lut["species"].str.replace("/", ",")).str.split(",").tolist()
        )  # list of lists.

        all_species = list(
            set(
                [
                    species.lower().strip()
                    for species_lst in aa_species_lst
                    for species in species_lst
                ]
            )
        )
        full_species_lst += all_species
    full_species_lst = list(set(full_species_lst))

    all_fia_names = fia.loc[np.isnan(fia["YEAR_DROPPED"]), "COMMON_NAME"].tolist()

    spp_name_to_fia_code = {}
    for spp in full_species_lst:
        matches = process.extract(spp, all_fia_names, limit=1)
        for match in matches:
            fia_spp_name, conf = match
            if conf > 80:
                fia_code = fia[fia["COMMON_NAME"] == fia_spp_name].iloc[0][
                    "FHM_SPECIES"
                ]
                spp_name_to_fia_code[
                    spp
                ] = fia_code.item()  # `.item()` casts from numpy.int to python int

    return spp_name_to_fia_code