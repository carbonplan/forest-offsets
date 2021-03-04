import json
import os
from functools import lru_cache

import fsspec
import geopandas
from carbonplan_data import cat as core_cat

from carbonplan_retro.analysis import rfia
from carbonplan_retro.arbitrage.prism_arbitrage import load_prism_arbitrage
from carbonplan_retro.data import cat
from carbonplan_retro.load.geometry import load_project_geometry


@lru_cache(maxsize=None)  # should cache via intake but whatever
def get_crs():
    da = core_cat.nlcd.raster(region="conus").to_dask()
    crs = da.attrs["crs"]
    return crs


def get_project_arbitrage(project):
    crs = get_crs()

    project_geom = load_project_geometry(project['opr_id'])

    # buffer so project doesnt slip through cracks in sampling grid
    project_geom = project_geom.to_crs(crs).buffer(16_000)

    arbitrage = []
    for supersection_id in project['supersection_ids']:

        ss_summary = rfia.summarize_project_by_ss(project, supersection_id)
        ss_acreage = sum([site_class['acreage'] for site_class in ss_summary])
        ss_fraction = ss_acreage / project['acreage']

        arbitrage_map = load_prism_arbitrage(supersection_id).to_crs(crs)
        project_arbitrage = geopandas.clip(arbitrage_map, project_geom)[
            ['delta_slag', 'mean_local_slag', 'relative_slag']
        ].mean()
        arbitrage.append(project_arbitrage * ss_fraction)

    return sum(arbitrage).to_dict()


if __name__ == '__main__':
    arbitrage = {}
    retro_json = cat.retro_db_light_json.read()

    projects = [project for project in retro_json if len(project['assessment_areas']) > 0]

    # exclude the three alaska assessment areas because no arbitrage maps up there
    # projects = [project for project in projects if 285 not in project['supersection_ids']]
    # projects = [project for project in projects if 286 not in project['supersection_ids']]
    # projects = [project for project in projects if 287 not in project['supersection_ids']]

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
