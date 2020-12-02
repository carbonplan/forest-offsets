def load_ak_supersection():
    ss_cd_map = load_ss_codes()

    return gpd.read_file(
        '/Users/darryl/proj/carbonplan/retro/data/geometry/ak.se.sc.supersection.shp.5.4.15'
    )


def load_arb_shapes(postal_code):
    if postal_code == 'ak':
        gdf = gpd.read_file('/home/jovyan/carbonplan/forests/notebooks/ak_assessment_areas/')
    else:
        gdf = gpd.read_file('/home/jovyan/carbonplan/forests/notebooks/2015_arb_supersections/')
    return gdf.to_crs('epsg:4326')


def load_omernik(postal_code):
    if postal_code == 'ak':
        omernik = gpd.read_file('/home/jovyan/carbonplan/retro/data/ak_omernik/')
    else:
        raise NotImplementedError('only have Omernik for AK right now')
    return omernik.to_crs('epsg:4326')


def load_ecomap(postal_code):
    if postal_code == 'ak':
        ecomap = gpd.read_file('/home/jovyan/carbonplan/retro/akecoregions-ShapeFile')
        ecomap = ecomap.set_crs('epsg:3338')
        ecomap = ecomap.dropna()
    else:
        ecomap = gpd.read_file(pathlib.Path(__file__).parent / 'data/geometry/S_USA.EcomapSections')
        ecomap = ecomap.set_index('MAP_UNIT_S')

    return ecomap.to_crs('epsg:4326')


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

    return gdf
