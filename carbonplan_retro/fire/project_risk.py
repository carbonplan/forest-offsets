import os
from functools import lru_cache

import fire
import fsspec
import geopandas as gp
import regionmask as rm
import xarray as xr
from shapely.geometry import Point


def integrated_risk(p):
    from scipy.stats import binom

    return 1 - binom.cdf(0, 100, p)


def average_risk(da, mask):
    return (
        da.where(mask)
        .mean(['x', 'y'])
        .groupby('time.year')
        .sum()
        .sel(year=slice('2001', '2018'))
        .mean()
    )


def query_by_region(da, lon, lat, regions):
    p = Point(lon, lat)
    masks = rm.mask_3D_geopandas(regions, da, drop=False)
    region = [i for i in regions.index if regions.geometry[i].contains(p)][0]
    return average_risk(da, masks[region])


def query_by_shape(da, shape):
    mask = rm.mask_3D_geopandas(shape.simplify(0.002).buffer(0.005), da)[0]
    return average_risk(da, mask)


def query_by_location(da, lon, lat):
    latgrid = da['lat']
    longrid = da['lon']
    dist = (longrid - lon) ** 2 + (latgrid - lat) ** 2
    mask = dist < 0.5
    return average_risk(da, mask)


@lru_cache(maxsize=None)
def get_mtbs(prefix, load=True):
    mapper = fsspec.get_mapper(prefix + '/processed/mtbs/conus/4000m/monthly.zarr')
    da = xr.open_zarr(mapper, consolidated=True)['monthly']
    if load:
        print('loading mtbs')
        da = da.load()
    return da


@lru_cache(maxsize=None)
def get_supersections(prefix):
    regions = gp.read_file(prefix + '/raw/ecoregions/supersections.geojson')
    return regions


@lru_cache(maxsize=None)
def get_baileys(prefix):
    regions = gp.read_file(prefix + '/raw/ecoregions/baileys.geojson')
    return regions


def project_risk(store='local', mode='supersection', lat=None, lon=None, id=None):
    if store == 'local':
        prefix = os.path.expanduser('~/workdir/carbonplan-data')

    if store == 'gs':
        prefix = 'https://storage.googleapis.com/carbonplan-data'

    da = get_mtbs(prefix)

    if mode == 'supersection':
        regions = get_supersections(prefix)
        risk = integrated_risk(query_by_region(da, lon, lat, regions))
        print(risk)

    elif mode == 'baileys':
        regions = get_baileys(prefix)
        risk = integrated_risk(query_by_region(da, lon, lat, regions))
        print(risk)
    elif mode == 'shape':
        shape = gp.read_file(prefix + f'/raw/projects/{id}.json')
        risk = integrated_risk(query_by_shape(da, shape))
        print(risk)
    elif mode == 'location':
        risk = integrated_risk(query_by_location(da, lon, lat))
        print(risk)
    else:
        raise ValueError('invalid mode')

    return risk


if __name__ == '__main__':
    fire.Fire(project_risk)
