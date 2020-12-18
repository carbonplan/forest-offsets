import json
import pathlib

import geopandas as gpd
import pandas as pd
from pandas import MultiIndex, read_json


def load_retro_from_json(fname):
    strip = lambda x: x.strip()

    def str_to_tuple(s):
        return tuple(map(strip, s[1:-1].replace("'", "").split(',')))

    df = read_json(fname, orient='index', convert_dates=True)
    df.columns = MultiIndex.from_tuples(map(str_to_tuple, df.columns))
    return df


def get_closest_supersection(supersection_shps, proj):
    """"""
    if len(proj.supersections) > 1:
        return proj.supersections
    fn = pathlib.Path(__file__).parent / 'data/border_dict.json'
    with open(fn) as f:
        border_lst = json.load(f).get(str(proj.supersections[0]))
    closest = supersection_shps.loc[border_lst].distance(proj.geom).sort_values().idxmin()
    return [closest]


def get_state_boundaries(state_abbrs: list):
    """helper to filter things by state"""

    states = gpd.read_file(
        "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_1_states_provinces.zip"
    )

    states = states[states.postal.isin([x.upper() for x in state_abbrs])]
    return states[['name', 'postal', 'geometry']]


def load_aa_to_ss_map():
    fn = pathlib.Path(__file__).parents[1] / 'data/2015_aa_lut.csv'
    df = pd.read_csv(fn, usecols=['aa_code', 'ss_code'])
    return df.set_index('aa_code')['ss_code'].to_dict()


def load_ss_codes():
    ss_cd_map = pd.read_csv(
        "/Users/darryl/proj/carbonplan/retro/data/ancillary/super_section_lookup.csv",
        names=["code", "ss"],
    )

    return ss_cd_map.set_index("ss")["code"].to_dict()


def load_aa_codes():
    aa_code_map = pd.read_csv(
        "/Users/darryl/forest-retro/ancillary-files/assessment_area_lookup.csv",
        names=["code", "aa"],
    )
    return aa_code_map.set_index("aa")["code"].to_dict()


def get_arb_id_map():
    return (
        pd.read_csv(
            '/Users/darryl/proj/carbonplan/retro/data/issuance.csv', usecols=['arb_id', 'proj_id']
        )
        .set_index('arb_id')
        .proj_id.to_dict()
    )
