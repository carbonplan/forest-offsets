import geopandas as gpd
import pandas as pd


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

def load_supersection_shapes():
    ss_cd_map = load_ss_codes()

    # the 2015 arb shapefile misspells a few supersections so fix those here
    typos = {"MW Broadleaf Forest SC Great Lakes & Lake Whittlesey": "MW Broadleaf Forest SC Great Lakes & Lake Whittles",
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


def get_arb_id_map():
    return pd.read_csv('../data/issuance.csv', usecols=['arb_id', 'proj_id']).set_index('arb_id').proj_id.to_dict()