import pathlib

from pandas import MultiIndex, read_json

import geopandas as gpd
import pandas as pd
import json


def load_retro_from_json(fname):
    strip = lambda x: x.strip()

    def str_to_tuple(s):
        return tuple(map(strip, s[1:-1].replace("'", "").split(',')))

    df = read_json(fname, orient='index', convert_dates=True)
    df.columns = MultiIndex.from_tuples(map(str_to_tuple, df.columns))
    return df


PROJECTED_CRS = 'PROJCRS["NAD_1983_Albers",BASEGEOGCRS["NAD83",DATUM["North American Datum 1983",ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["Degree",0.0174532925199433]],ID["EPSG",4269]],CONVERSION["unnamed",METHOD["Albers Equal Area",ID["EPSG",9822]],PARAMETER["Easting at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8827]],PARAMETER["Longitude of false origin",-96,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",29.5,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45.5,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Latitude of false origin",23,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]]],CS[Cartesian,2],AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1,ID["EPSG",9001]]],AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1,ID["EPSG",9001]]]]'
GEOGRAPHIC_CRS = 'epsg:4326'


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

    states = gpd.read_file(
        "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_1_states_provinces.zip"
    )

    states = states[states.postal.isin([x.upper() for x in state_abbrs])]
    return states


def load_aa_to_ss_map():
    fn = pathlib.Path(__file__).parent / 'data/2015_aa_lut.csv'
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


def load_ak_supersection():
    ss_cd_map = load_ss_codes()

    return gpd.read_file(
        '/Users/darryl/proj/carbonplan/retro/data/geometry/ak.se.sc.supersection.shp.5.4.15'
    )


def load_supersection_shapes():
    ss_cd_map = load_ss_codes()

    # the 2015 arb shapefile misspells a few supersections so fix those here
    typos = {
        "MW Broadleaf Forest SC Great Lakes & Lake Whittlesey": "MW Broadleaf Forest SC Great Lakes & Lake Whittles",
        "Prairie Parkland Central Till Plains & Grand Prairies": "Prairie Parkland Central Till Plains & Grand",
        "Central Interior Broadleaf Forest Central Till Plains": "Central Interior Broadleaf Forest Central Till",
        "Central Interior Broadleaf Forest Eastern Low Plateau": "Central Interior Broadleaf Forest Eastern Low",
        "Central Interior Broadleaf Forest Western Low Plateau": "Central Interior Broadleaf Forest Western Low",
        "Eastern Broadleaf Forest Cumberland Plateau & Valley": "Eastern Broadleaf Forest Cumberland Plateau",
    }
    for proper_spelling, typo_spelling in typos.items():
        ss_cd_map[typo_spelling] = ss_cd_map[proper_spelling]

    ss = gpd.read_file('/Users/darryl/proj/carbonplan/retro/data/geometry/2015_arb_supersections')
    ss["ss_id"] = ss["SSection"].map(ss_cd_map)
    ss = ss.set_index('ss_id')
    return ss

def load_ecomap_shapes():

    gdf = gpd.read_file(pathlib.Path(__file__).parent / 'data/geometry/S_USA.EcomapSections')
    gdf = gdf.set_index('MAP_UNIT_S')
    return gdf

def get_arb_id_map():
    return (
        pd.read_csv('../data/issuance.csv', usecols=['arb_id', 'proj_id'])
        .set_index('arb_id')
        .proj_id.to_dict()
    )
