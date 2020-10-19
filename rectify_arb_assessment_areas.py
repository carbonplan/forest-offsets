import json

import pandas as pd

import species_matching



class AssessmentAreaLUT(object):
    """ Wrapper around LUT with convenience methods to do aa/ss lookups

    """
    def __init__(self,fname):
        self.df = pd.read_csv(fname)
        with open('data/species_name_to_fia_code.json') as f:
            self.fia_species_map = json.load(f)

    def get_unique_aa(self, ss_code):
        """ Given a ss_code, return unique A
         """
        aa_codes = self.df.loc[self.df["ss_code"] == ss_code].aa_code.unique()
        return aa_codes

    def get_aa_species_lst(self, aa_code):
        species_str = self.df.loc[self.df['aa_code'] == aa_code, 'species'].item()
        species_lst = species_matching.clean_species_str(species_str)
        encoded_lst = species_matching.encode_species_lst(species_lst, self.fia_species_map)
        return encoded_lst
