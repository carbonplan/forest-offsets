import json
import geopandas as gpd

from utils import load_ss_codes


def repackage_arb_supersections():
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
    ss.to_file('../data/arb_supersections.json', driver='GeoJSON')


def main():
    repackage_arb_supersections()


if __name__ == "__main__":
    main()
