import math
from functools import lru_cache

import fsspec
import geopandas
import numpy as np
import pandas as pd
import rioxarray  # noqa
import xarray as xr
from shapely.geometry import Point
from sklearn.neighbors import KDTree
from sklearn.preprocessing import QuantileTransformer

from ..data import cat, get_retro_bucket
from ..load.fia import load_fia_common_practice
from ..load.geometry import get_bordering_supersections, get_overlapping_states, load_supersections


@lru_cache(maxsize=None)
def load_prism(region, var):
    return cat.prism(region=region, var=var).read().squeeze().rename(var)


def load_prism_arbitrage(supersection_id):
    fs_prefix, fs_kwargs = get_retro_bucket()
    fn = f'{fs_prefix}/arbitrage/prism-supersections/{supersection_id}.json'
    with fsspec.open(fn, mode='r', **fs_kwargs) as f:
        mesh = geopandas.read_file(f)
    return mesh


def get_prism_arbitrage_map(supersection_id):
    supersections = load_supersections()  # .to_crs(crs)
    supersection = supersections[supersections["ss_id"] == supersection_id]

    if supersection_id > 200:
        region = 'ak'
        postal_codes = ['ak']
    else:
        region = 'conus'
        bordering_supersections = get_bordering_supersections([supersection_id])
        aoi = bordering_supersections.unary_union
        postal_codes = get_overlapping_states(aoi)

    tmean = load_prism(region, "tmean")
    precip = load_prism(region, "ppt")
    prism = xr.merge([tmean, precip], combine_attrs="override")

    projected_crs = tmean.crs

    clipped = prism.rio.clip(supersection.to_crs(tmean.crs).geometry)

    clim_df = clipped.where(clipped > -9999).to_dataframe().dropna().reset_index()

    t_transformer = QuantileTransformer(n_quantiles=1_000)
    p_transformer = QuantileTransformer(n_quantiles=1_000)

    clim_df["tmean_q"] = t_transformer.fit_transform(
        clim_df["tmean"].values.reshape(-1, 1)
    ).squeeze()
    clim_df["ppt_q"] = p_transformer.fit_transform(clim_df["ppt"].values.reshape(-1, 1)).squeeze()

    clim_points = [Point(x, y) for x, y in zip(clim_df.x.values, clim_df.y.values)]
    gdf = geopandas.GeoDataFrame(clim_df[["tmean_q", "ppt_q"]], geometry=clim_points, crs=tmean.crs)
    climate_kdtree = KDTree(np.stack([gdf.geometry.x.values, gdf.geometry.y.values], axis=1))

    fia_df = load_fia_common_practice(postal_codes)
    fia_df = fia_df[(fia_df["MEASYEAR"] > 2001) & (fia_df["MEASYEAR"] <= 2013)]
    fia_df = fia_df.to_crs(gdf.crs)

    points = np.array(list(fia_df.geometry.apply(lambda p: (p.x, p.y))))

    inds = climate_kdtree.query(points, return_distance=False).squeeze()

    fia_clim = pd.concat(
        [
            fia_df.reset_index(drop=True),
            gdf.loc[inds][["tmean_q", "ppt_q"]].reset_index(drop=True),
        ],
        axis=1,
    )

    ss_climate = (
        clipped.where(clipped["ppt"] > -9999)
        .where(clipped["tmean"] > -9999)
        .to_dataframe()
        .dropna()
        .reset_index()
    )

    ss_points = [Point(x, y) for x, y in zip(ss_climate.x.values, ss_climate.y.values)]
    ss_climate = geopandas.GeoDataFrame(data=ss_climate, geometry=ss_points, crs=prism.crs)

    ss_climate["tmean_q"] = t_transformer.transform(
        ss_climate["tmean"].values.reshape(-1, 1)
    ).squeeze()
    ss_climate["ppt_q"] = p_transformer.transform(ss_climate["ppt"].values.reshape(-1, 1)).squeeze()

    fia_tree = KDTree(np.stack([fia_clim.tmean_q.values, fia_clim.ppt_q.values], axis=1))

    mesh_sample_points = np.array(list(ss_climate.apply(lambda x: (x.tmean_q, x.ppt_q), axis=1)))

    fia_idx = fia_tree.query(
        mesh_sample_points, k=math.floor(0.1 * len(fia_clim)), return_distance=False
    )

    mean_slag = [fia_clim.loc[idx, "slag_co2e_acre"].mean() for idx in fia_idx]
    ss_climate["mean_local_slag"] = mean_slag

    # clip FIA back down to just the supersection in question; lookup can span outside -- arbitrage cannot
    clipped_fia = geopandas.clip(fia_clim, supersection.to_crs(projected_crs))

    ss_climate["delta_slag"] = ss_climate["mean_local_slag"] - clipped_fia["slag_co2e_acre"].mean()
    ss_climate["relative_slag"] = (
        ss_climate["mean_local_slag"] / clipped_fia["slag_co2e_acre"].mean()
    )
    return ss_climate


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
        286,
        287,
    ]
    fs_prefix, fs_kwargs = get_retro_bucket()

    for supersection_id in supersections_with_projects:
        arbitrage_map = get_prism_arbitrage_map(supersection_id)

        fn = f'{fs_prefix}/arbitrage/prism-supersections/{supersection_id}.json'
        with fsspec.open(fn, mode='w', **fs_kwargs) as f:
            f.write(arbitrage_map.to_crs('epsg:4326').to_json())
