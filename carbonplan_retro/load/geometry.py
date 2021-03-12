import pathlib
from functools import lru_cache

import geopandas as gp
import pandas as pd
from shapely.ops import cascaded_union
from tenacity import retry, stop_after_attempt, wait_fixed

from carbonplan_retro.data import cat

from ..utils import supersection_str_to_ss_code


@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
def load_project_geometry(opr_id):
    try:
        return cat.arb_geometries(opr_id=opr_id).read()
    except:
        raise


def load_ak_supersections():
    '''load alaska assessment areas and rearrange so can append to CONUS supersections'''
    gdf = cat.ak_assessment_areas.read()
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


@lru_cache(maxsize=None)
def load_supersections(include_ak=True, fix_typos=True):
    str_to_code = supersection_str_to_ss_code()

    gdf = cat.supersections.read()

    if fix_typos:
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
            # bit clunky -- append another k:v pair to the dict that maps shapefile_spelling to assessment spelling
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


@lru_cache(maxsize=None)
def load_states():
    states = cat.states.read()
    states = states[states['postal'] != 'DC']  # never want to consider DC
    return states


def get_overlapping_states(geometry):
    states = load_states()
    return states[states.intersects(geometry)]['postal'].str.lower().to_list()


def get_bordering_supersections(supersection_ids: list):
    supersections = load_supersections()
    subset = supersections[supersections.ss_id.isin(supersection_ids)]
    geom = cascaded_union(subset['geometry'])
    # buffer slightly to avoid touches/overlaps confusion [if just touch but no intersect overlaps wont return]
    overlapping = supersections[supersections.geometry.overlaps(geom.buffer(0.01))]
    return pd.concat([subset, overlapping])
