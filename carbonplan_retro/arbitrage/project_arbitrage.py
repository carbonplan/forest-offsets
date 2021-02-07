from functools import lru_cache

import geopandas
import numpy as np
from carbonplan_data import cat as core_cat

from carbonplan_retro.arbitrage.assessment_area_arbitrage import load_assessment_area_arbitrage
from carbonplan_retro.data import cat


@lru_cache(maxsize=None)  # should cache via intake but whatever
def get_crs():
    da = core_cat.nlcd.raster(region="conus").to_dask()
    crs = da.attrs["crs"]
    return crs


def get_project_arbitrage(project):
    crs = get_crs()
    project_geom = (
        cat.arb_geometries(opr_id=project['opr_id']).read().to_crs(crs).buffer(16_000)
    )  # 8km

    arbitrage = []
    for assessment_area in project['assessment_areas']:
        if assessment_area['code'] != 999:
            fraction = assessment_area['acreage'] / project['acreage']
            arbitrage_map = load_assessment_area_arbitrage(assessment_area['code']).to_crs(crs)
            project_aa_arbitrage = geopandas.clip(arbitrage_map, project_geom)[
                ['ds', 'mls', 'rs']
            ].mean()
            if np.any(np.isnan(project_aa_arbitrage)):
                raise ValueError(
                    f'{project["opr_id"]} has a bum arbitrage score for assessment area {assessment_area["code"]}'
                )
            arbitrage.append(project_aa_arbitrage * fraction)

    return sum(arbitrage).to_dict()


if __name__ == '__main__':
    arbitrage = {}
    for project in cat.retro_db_light_json.read():
        arbitrage_scores = get_project_arbitrage(project)
        arbitrage[project['opr_id']] = arbitrage_scores
