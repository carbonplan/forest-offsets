import json
import os
from collections import defaultdict

import dask
import fsspec
from dask.distributed import Client

from carbonplan_retro.analysis import rfia
from carbonplan_retro.analysis.common_practice import get_arbocs
from carbonplan_retro.data import cat


def get_slag_to_total_scalar(project, use_baseline=False):
    """baseline is constrained by SLAG but project is creditted for above and below, this function
    infers the relationship between SLAG and IFM_1 using project data
    """
    if use_baseline:
        return project['baseline']['ifm_1'] / (
            project['average_slag_baseline'] * project['acreage']
        )
    else:
        return project['rp_1']['ifm_1'] / (
            project['carbon']['initial_carbon_stock']['value'] * project['acreage']
        )


def get_recalculated_arbocs(project, alternate_cp):
    """given a baseline and a minimum cp, calculate arbocs
    assumes that IFM_1 is a linear function of CP, which is what we see in practice
    """
    alt_baseline = project['baseline'].copy()
    slag_to_total_carbon = get_slag_to_total_scalar(project, use_baseline=False)
    alt_baseline['ifm_1'] = project['acreage'] * alternate_cp * slag_to_total_carbon

    alt_arbocs = get_arbocs(alt_baseline, project['rp_1'])
    return alt_arbocs


@dask.delayed(pure=True, traverse=True)
def get_project_overcrediting(project, fortyp_weights, n_obs=1_000):
    store = defaultdict(list)
    i = 0

    baseline_rfia_slag = rfia.get_rfia_arb_common_practice(project, use_site_class='all')

    with dask.config.set(scheduler='single-threaded'):
        while i < n_obs:
            alt_slag = rfia.get_project_weighted_slag(
                project, fortyp_weights, use_site_class='all', uncertainty=True
            )
            scaled_alt_slag = (
                project['carbon']['common_practice']['value'] * alt_slag / baseline_rfia_slag
            )

            # CAR1183 has CP of zero.
            if scaled_alt_slag == 0:
                scaled_alt_slag = alt_slag

            alt_arbocs = get_recalculated_arbocs(project, scaled_alt_slag)

            if scaled_alt_slag > project['carbon']['initial_carbon_stock']['value']:
                alt_arbocs = 0

            delta_arbocs = project['arbocs']['calculated'] - alt_arbocs
            store['alt_slag'].append(scaled_alt_slag)
            store['alt_arbocs'].append(alt_arbocs)
            store['delta_arbocs'].append(delta_arbocs)
            i += 1
    return store


if __name__ == '__main__':

    client = Client(threads_per_worker=1)
    print(client)
    print(client.dashboard_link)

    retro_json = cat.retro_db_light_json.read()
    projects = [
        project
        for project in retro_json
        if project['carbon']['initial_carbon_stock']['value']
        > project['carbon']['common_practice']['value']
    ]

    with fsspec.open(
        'az://carbonplan-retro/classifications.json',
        account_key=os.environ["BLOB_ACCOUNT_KEY"],
        account_name="carbonplan",
        mode='r',
    ) as f:
        reclassification_weights = json.load(f)

    overcrediting = {}
    for project in projects:
        pid = project['opr_id']

        if pid in reclassification_weights:
            fortyp_weights = reclassification_weights[pid]
        else:
            continue

        overcrediting[pid] = get_project_overcrediting(project, fortyp_weights)

    overcrediting = dask.compute(overcrediting)

    # @badgley fix me
    with fsspec.open(
        'az://carbonplan-scratch/overcredited_arbocs.json',
        account_key=os.environ["BLOB_ACCOUNT_KEY"],
        account_name="carbonplan",
        mode='w',
    ) as f:
        json.dump(overcrediting[0], f)
