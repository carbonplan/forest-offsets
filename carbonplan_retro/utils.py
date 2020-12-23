import json
import pathlib

import geopandas
import pandas as pd

from .data import cat


def load_retro_from_json(fname: str) -> pd.DataFrame:
    """ Load retro-db from JSON """

    strip = lambda x: x.strip()

    def str_to_tuple(s):
        return tuple(map(strip, s[1:-1].replace("'", "").split(',')))

    df = pd.read_json(fname, orient='index', convert_dates=True)
    df.columns = pd.MultiIndex.from_tuples(map(str_to_tuple, df.columns))
    return df


def get_closest_supersection(supersection_shps, proj) -> list:
    """ Get the closest supersection to a project"""
    if len(proj.supersections) > 1:
        return proj.supersections
    fn = pathlib.Path(__file__).parent / 'data/border_dict.json'
    with open(fn) as f:
        border_lst = json.load(f).get(str(proj.supersections[0]))
    closest = supersection_shps.loc[border_lst].distance(proj.geom).sort_values().idxmin()
    return [closest]


def get_state_boundaries(state_abbrs: list) -> geopandas.GeoDataFrame:
    """helper to get a  things by state"""
    states = cat.states.read()
    states = states[states.postal.isin([x.upper() for x in state_abbrs])]
    return states[['name', 'postal', 'geometry']]


def load_aa_to_ss_map() -> dict:
    fn = pathlib.Path(__file__).parents[1] / 'data/2015_aa_lut.csv'
    df = pd.read_csv(fn, usecols=['aa_code', 'ss_code'])
    return df.set_index('aa_code')['ss_code'].to_dict()


def load_ss_codes() -> dict:
    ss_cd_map = cat.super_section_lookup.read()
    return ss_cd_map.set_index("ss")["code"].to_dict()


def load_aa_codes() -> dict:
    aa_code_map = cat.assessment_area_lookup.read()

    return aa_code_map.set_index("aa")["code"].to_dict()
