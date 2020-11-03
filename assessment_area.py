import json

from itertools import chain
import pandas as pd

import species_matching

# TODO: This probably shouldnt be here but whatever
# TODO: Ask Joe/Jeremy on how to handle this -- dont want to reload 100x
with open("data/species_name_to_fia_code.json") as f:
    FIA_SPECIES_MAP = json.load(f)


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


def encode_species_lst(spp_lst):
    return [FIA_SPECIES_MAP[spp] for spp in spp_lst if spp in FIA_SPECIES_MAP]


def get_species_lst(species_str):
    """
    Translate ARB assessment area species string to (999,) vector for similarity scoring
    """
    species_lst = clean_species_str(species_str)
    encoded_lst = encode_species_lst(species_lst)
    return encoded_lst




class AssessmentAreaLUT(object):
    """Wrapper around LUT with convenience methods to do aa/ss lookups"""
    _data = None

    def __init__(self, fname):
        self._load_data(fname)

    def _load_data(self, fname):
        self._data = pd.read_csv(fname)
        self._data['species_lst'] = self._data['species'].apply(get_species_lst)

    def get_supersection_assessment_areas(self, ss_code):
        """Given a ss_code, return unique assesment areas"""
        aa_codes = self._data.loc[self._data["ss_code"] == ss_code].aa_code.unique()
        return aa_codes

    def get_aa_species_lst(self, aa_code):
        return self._data.loc[self._data['aa_code'] == aa_code].max()['species_lst']

    def get_common_practice(self, aa_code, site_class=None):
        cps = self._data.loc[self._data['aa_code'] == aa_code].set_index('site_class')['common_practice'].to_dict()
        if site_class:
            try:
                return cps[site_class]
            except KeyError: # reassigned to an assessment area without high/low site_class!
                return cps['All']
            except:
                raise
        return cps




