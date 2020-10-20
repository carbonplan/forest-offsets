import geopandas as gpd
import pandas as pd


def load_ss_codes():
    ss_cd_map = pd.read_csv(
        "/Users/darryl/forest-retro/ancillary-files/super_section_lookup.csv",
        names=["code", "ss"],
    )

    return ss_cd_map.set_index("ss")["code"].to_dict()


def load_aa_codes():
    aa_code_map = pd.read_csv(
        "/Users/darryl/forest-retro/ancillary-files/assessment_area_lookup.csv",
        names=["code", "aa"],
    )
    return aa_code_map.set_index("aa")["code"].to_dict()

def load_supersection_shapes():
    ss_cd_map = load_ss_codes()
    ss = gpd.read_file('/Users/darryl/proj/carbonplan/retro/data/geometry/2015_arb_supersections')
    ss["ss_id"] = ss["SSection"].map(ss_cd_map)
    return ss


def get_arb_id_map():
    return pd.read_csv('../data/issuance.csv', usecols=['arb_id', 'proj_id']).set_index('arb_id').proj_id.to_dict()