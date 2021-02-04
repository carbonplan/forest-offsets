import geopandas
import numpy as np

from carbonplan_retro.arbitrage.assessment_area_arbitrage import load_assessment_area_arbitrage
from carbonplan_retro.data import cat


def get_project_arbitrage(project):
    project_geom = cat.arb_geometries(opr_id=project['opr_id']).read()

    arbitrage = []
    for assessment_area in project['assessment_areas']:
        fraction = assessment_area['acreage'] / project['acreage']
        arbitrage_map = load_assessment_area_arbitrage(assessment_area['code'])
        project_aa_arbitrage = geopandas.clip(arbitrage_map, project_geom)['rs'].mean()
        if np.isnan(project_aa_arbitrage) or project_aa_arbitrage < 0.05:
            raise ValueError(
                f'{project["opr_id"]} has a bum arbitrage score for assessment area {assessment_area["code"]}'
            )

        arbitrage.append(project_aa_arbitrage * fraction)

    return sum(arbitrage)
