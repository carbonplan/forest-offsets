import os

import fsspec
import geopandas
import numpy as np
from sklearn.neighbors import KDTree

from carbonplan_retro.arbitrage.arbitrage_mesh import load_conus_mesh, load_supersection_mesh
from carbonplan_retro.load.arb_fortypcds import load_arb_fortypcds
from carbonplan_retro.load.fia import load_fia_common_practice
from carbonplan_retro.load.geometry import get_overlapping_states, load_supersections
from carbonplan_retro.utils import aa_code_to_ss_code


def load_regional_common_practice(geometry):
    postal_codes = get_overlapping_states(geometry)
    df = load_fia_common_practice(postal_codes)
    df = df[(df['MEASYEAR'] > 2000) & (df['MEASYEAR'] < 2015)]

    return df


def get_neighborhood_slag(sample_mesh, data, k_neighbors=200, max_radius=250_000):
    # KDtree of observed data so we can look up neighbors for all points on grid
    tree = KDTree(np.stack([data.geometry.x.values, data.geometry.y.values], axis=1))

    # at each point in sample mesh, sample nearest data and take mean slag
    mean_slag = []
    for p in sample_mesh.geometry:
        xy = p.x, p.y
        distances, inds = tree.query([xy], k=min(k_neighbors, len(data)), return_distance=True)
        # Limit distance to prevent straying outside the neighborhood too far.  Not strictly necessary.
        radius_inds = inds[distances < max_radius]
        mean_slag.append(data["slag_co2e_acre"].iloc[radius_inds].mean())
    return mean_slag


def load_assessment_area_arbitrage(assessment_area_id):
    try:
        bucket = 'az://carbonplan-retro/arbitrage/assessment_areas/'
        fn = os.path.join(bucket, f"{assessment_area_id}.json")

        with fsspec.open(
            fn, account_name='carbonplan', mode='r', account_key=os.environ['BLOB_ACCOUNT_KEY']
        ) as f:
            # geojson store col names for every row. shorten to be nice in the event we pull right into the browser
            mesh = geopandas.read_file(f)
    except:
        mesh = create_assessment_area_arbitrage(assessment_area_id)
    return mesh


def create_assessment_area_arbitrage(assessment_area_id, save=False):

    mesh = load_conus_mesh()
    crs = mesh.crs

    fortypcds = load_arb_fortypcds().get(assessment_area_id)
    if not fortypcds:
        raise ValueError('Assessment area didnt map to any fortypcds')

    supersection_id = aa_code_to_ss_code().get(assessment_area_id)
    sample_mesh = load_supersection_mesh(supersection_id).to_crs(crs)

    # buffer out so can sample across borders of supersection bounds; we clip back down later.
    supersections = load_supersections().to_crs(crs)
    supersection = supersections.loc[supersections['ss_id'] == supersection_id].buffer(125_000)

    fia_data = load_regional_common_practice(supersection.to_crs('epsg:4326').geometry.item())
    fia_data = fia_data[fia_data['FORTYPCD'].isin(fortypcds)]

    fia_data = fia_data.to_crs(crs)

    mean_local_slag = get_neighborhood_slag(sample_mesh, fia_data)
    sample_mesh["mean_local_slag"] = mean_local_slag

    supersection_df = geopandas.clip(
        fia_data.reindex(), supersection
    )  # clip back down to ARB defined supersection domain

    sample_mesh["delta_slag"] = (
        sample_mesh["mean_local_slag"] - supersection_df["slag_co2e_acre"].mean()
    )
    sample_mesh["relative_slag"] = (
        sample_mesh["mean_local_slag"] / supersection_df["slag_co2e_acre"].mean()
    )

    geojson_names = {"mean_local_slag": 'mls', 'relative_slag': 'rs', 'delta_slag': 'ds'}

    sample_mesh = sample_mesh[
        ['geometry', 'mean_local_slag', 'relative_slag', 'delta_slag']
    ].rename(columns=geojson_names)

    if save:
        bucket = 'az://carbonplan-retro/arbitrage/assessment_areas/'
        fn = os.path.join(bucket, f"{assessment_area_id}.json")

        with fsspec.open(
            fn, account_name='carbonplan', mode='w', account_key=os.environ['BLOB_ACCOUNT_KEY']
        ) as f:
            # geojson store col names for every row. shorten to be nice in the event we pull right into the browser

            f.write(sample_mesh.to_crs('epsg:4326').to_json())

    return sample_mesh


if __name__ == '__main__':
    print("creating all relevant arbitrage maps")
    supersections_with_projects = [
        1,
        2,
        4,
        15,
        18,
        22,
        24,
        25,
        32,
        35,
        37,
        38,
        39,
        41,
        42,
        43,
        44,
        55,
        58,
        60,
        69,
        76,
        79,
        86,
        88,
        94,
        95,
    ]
    for supersection_id in supersections_with_projects:
        assessment_area_ids = [
            int(aa_id) for aa_id, ss_id in aa_code_to_ss_code().items() if ss_id == supersection_id
        ]
        for assessment_area_id in assessment_area_ids:
            print(f"creating {assessment_area_id}")
            create_assessment_area_arbitrage(assessment_area_id, save=True)
