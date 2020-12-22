import json
from functools import lru_cache

import pandas as pd


@lru_cache(maxsize=None)
def get_fia_species_map():
    with open("data/species_name_to_fia_code.json") as f:
        return json.load(f)


def compare_species_lists(comparison_lst: list, benchmark_lst: list):
    return len([species for species in comparison_lst if species in benchmark_lst])


def clean_species_str(species_str):
    black_lst = [
        "upland",
        "",
        "lowland",
        "tropical hardwoods",  # too vague to map, and unused...
        "mixed upland hardwoods",
    ]
    species_lst = species_str.split(",")
    return [species.strip().lower() for species in species_lst if species not in black_lst]


def encode_species_lst(spp_lst):
    fia_species_map = get_fia_species_map()
    return [fia_species_map[spp] for spp in spp_lst if spp in fia_species_map]


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

    def _list_assessment_areas(self, ss_code):
        """Given a ss_code, return unique assesment areas"""
        aa_codes = self._data.loc[self._data["ss_code"] == ss_code].aa_code.unique()
        return aa_codes

    def get_aa_species_lst(self, aa_code):
        return self._data.loc[self._data['aa_code'] == aa_code].max()['species_lst']

    def get_common_practice(self, aa_code, site_class=None):
        cps = (
            self._data.loc[self._data['aa_code'] == aa_code]
            .set_index('site_class')['common_practice']
            .to_dict()
        )

        if site_class:
            try:
                return cps[site_class]
            except KeyError:
                # reassigned to an assessment area without high/low site_class!
                if site_class in ["high", "low"]:
                    return cps['all']
                # reassigned from All to High/Low
                if site_class == 'all':
                    return sum([val for val in cps.values()]) / len(cps)
            except:
                raise
        else:
            return cps

    def get_assessment_areas(self, ss_code):
        """For superseciton, generate list of assessment_area dicts."""
        aa_codes = self._list_assessment_areas(ss_code)
        # NB -- this collapses high/low site class.
        candidates = [
            {'code': aa_code, 'species_lst': self.get_aa_species_lst(aa_code)}
            for aa_code in aa_codes
        ]
        return candidates

    def cross_map_ss_assesment_areas(self, from_ss, to_ss):
        """take all aa within `from_ss` and assign to an aa in `to_ss`"""
        from_assessment_areas = self.get_assessment_areas(from_ss)

        for from_assessment_area in from_assessment_areas:
            pass
