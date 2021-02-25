import json
import os
from functools import lru_cache

import fsspec
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
    # buffer to ensure we dont miss a sampled point in the sampling grid; some projects are quite small relative
    project_geom = cat.arb_geometries(opr_id=project['opr_id']).read().to_crs(crs).buffer(16_000)

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
    retro_json = cat.retro_db_light_json.read()

    projects = [project for project in retro_json if len(project['assessment_areas']) > 0]

    # exclude the three alaska assessment areas because no arbitrage maps up there
    projects = [project for project in projects if 285 not in project['supersection_ids']]
    projects = [project for project in projects if 286 not in project['supersection_ids']]
    projects = [project for project in projects if 287 not in project['supersection_ids']]

    for project in projects:
        print(project['opr_id'])
        arbitrage_scores = get_project_arbitrage(project)
        arbitrage[project['opr_id']] = arbitrage_scores

    with fsspec.open(
        "az://carbonplan-scratch/project_arbitrage.json",
        account_name="carbonplan",
        mode="w",
        account_key=os.environ["BLOB_ACCOUNT_KEY"],
    ) as f:
        json.dump(arbitrage, f)
