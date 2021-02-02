import os
from functools import lru_cache

import fsspec
import geopandas
from carbonplan.data import cat as core_cat
from shapely.geometry import Point

from carbonplan_retro.load.geometry import load_supersections


@lru_cache(maxsize=None)
def load_conus_mesh():
    ds = core_cat.grids.conus4k.to_dask()

    crs = ds.crs.attrs["crs_wkt"]
    mask = ds.mask.values.flat > 0
    points = [Point(x, y) for x, y in zip(ds.lon.values.flat[mask], ds.lat.values.flat[mask])]

    mesh = geopandas.GeoDataFrame(
        data={"num": list(range(len(points)))}, crs="epsg:4326", geometry=points
    )
    mesh = mesh.to_crs(crs=crs)
    return mesh


def get_supersection_mesh(supersection_id, save=False):
    mesh = load_conus_mesh()

    working_crs = mesh.crs

    supersections = (
        load_supersections(include_ak=False, fix_typos=True)
        .set_index('SSection')
        .to_crs(crs=working_crs)
    )

    supersection = supersections.loc[supersections['ss_id'] == supersection_id]

    supersection_mesh = geopandas.clip(mesh, supersection)

    if save:
        fn = f'az://carbonplan-retro/arbitrage/base_meshes/{int(supersection["ss_id"].item())}.json'
        with fsspec.open(
            fn, account_name='carbonplan', mode='w', account_key=os.environ['BLOB_ACCOUNT_KEY']
        ) as f:
            f.write(supersection_mesh.to_crs('wgs84').to_json())
    return supersection_mesh


if __name__ == '__main__':

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
        get_supersection_mesh(supersection_id, save=True)
