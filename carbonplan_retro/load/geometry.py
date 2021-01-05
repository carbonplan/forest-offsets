import pathlib

import geopandas as gp
import pandas as pd

from ..utils import supersection_str_to_ss_code


def load_ak_supersections():
    '''load alaska assessment areas and rearrange so can append to CONUS supersections'''
    gdf = gp.read_file(
        'https://ww3.arb.ca.gov/cc/capandtrade/protocols/usforest/2015/ak.assessment.area.shapefiles.5.4.15.zip'
    )
    # Refer to AK 'assessment areas' as three new 'supersections' that go by their assessment area names.
    gdf['SSection'] = gdf['Assessment']
    gdf = gdf.drop("Assessment", axis=1)

    # For the three AK assessment areas qua supersection, ss_id == aa_id.
    ak_supersection_str_to_ss_code = {
        'Alaska Range Transition': 285,
        'Alexander Archipelago - Kodiak': 286,
        'Gulf-NorthCoast-Chugach': 287,
    }
    gdf['ss_id'] = gdf['SSection'].map(ak_supersection_str_to_ss_code)

    return gdf.to_crs('epsg:4326')


def load_omernik(postal_code):
    if postal_code == 'ak':
        omernik = gp.read_file('/home/jovyan/carbonplan/retro/data/ak_omernik/')
    else:
        raise NotImplementedError('only have Omernik for AK right now')
    return omernik.to_crs('epsg:4326')


def load_ecomap(postal_code):
    ''''''
    if postal_code == 'ak':
        ecomap = gp.read_file('/home/jovyan/carbonplan/retro/akecoregions-ShapeFile')
        ecomap = ecomap.set_crs('epsg:3338')
        ecomap = ecomap.dropna()
    else:
        ecomap = gp.read_file(pathlib.Path(__file__).parent / 'data/geometry/S_USA.EcomapSections')
        ecomap = ecomap.set_index('MAP_UNIT_S')

    return ecomap.to_crs('epsg:4326')


def load_supersections(prefix=None, include_ak=True):
    str_to_code = supersection_str_to_ss_code()

    gdf = gp.read_file(prefix + '/raw/ecoregions/supersections.geojson')
    # the 2015 arb shapefile has spellings of supersections that differ from Assessment Area file.
    # we based numbering off the Assessment Area file, so need to change spelling :/
    typos = {
        "MW Broadleaf Forest SC Great Lakes & Lake Whittlesey": "MW Broadleaf Forest SC Great Lakes & Lake Whittles",
        "Prairie Parkland Central Till Plains & Grand Prairies": "Prairie Parkland Central Till Plains & Grand",
        "Central Interior Broadleaf Forest Central Till Plains": "Central Interior Broadleaf Forest Central Till",
        "Central Interior Broadleaf Forest Eastern Low Plateau": "Central Interior Broadleaf Forest Eastern Low",
        "Central Interior Broadleaf Forest Western Low Plateau": "Central Interior Broadleaf Forest Western Low",
        "Eastern Broadleaf Forest Cumberland Plateau & Valley": "Eastern Broadleaf Forest Cumberland Plateau",
        "Laurentian Mixed Forest Western Superior & Lake Plains": "Laurentian Mixed Forest Western Superior & Lake",
    }
    for assessment_lut_spelling, shapefile_spelling in typos.items():
        # bit of a cheat -- append another k:v pair to the dict that maps shapefile_spelling to assessment spelling
        str_to_code[shapefile_spelling] = str_to_code[assessment_lut_spelling]

    gdf["ss_id"] = gdf["SSection"].map(str_to_code)

    if include_ak:
        ak = load_ak_supersections()
        gdf = pd.concat([gdf, ak], ignore_index=True)

    return gdf


def ifm_shapes(opr_ids='all', load_series=True):
    # TODO: move somewhere @ config level.
    IFM_SHAPE_DIR = pathlib.Path(__file__).parents[2] / 'data/geometry/projects'
    if opr_ids == 'all':
        fns = IFM_SHAPE_DIR.glob('*.json')
    else:
        if isinstance(opr_ids, str):
            fns = [IFM_SHAPE_DIR / f'{opr_ids}.json']
        elif isinstance(opr_ids, list):
            fns = [IFM_SHAPE_DIR / f'{opr_id}.json' for opr_id in opr_ids]
        else:
            raise NotImplementedError("pass a single opr_id as a str or a list of opr_ids")

    project_shapes = {fn.stem: gp.read_file(fn) for fn in fns}
    df = pd.concat(project_shapes)

    df = df.droplevel(1)  # not sure where multi-index is coming from but ditch it here.
    if load_series:
        return df.geometry.reset_index().rename(columns={'index': 'opr_id'})
    else:
        return df
