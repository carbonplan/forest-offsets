import os

import fsspec
import geopandas as gp
import regionmask as rm
import xarray as xr
from shapely.geometry import Point

import fire


def integrated_risk(p):
    from scipy.stats import binom

    return (1 - binom.cdf(0, 100, p)) * 100


def average_risk(da, mask):
    return (
        da.where(mask)
        .mean(['x', 'y'])
        .groupby('time.year')
        .sum()
        .sel(year=slice('2001', '2018'))
        .mean()
    )


def query_by_ecoregion(da, lon, lat, regions):
    p = Point(lon, lat)
    masks = rm.mask_3D_geopandas(regions, da)
    ecoregion = [i for i in regions.index if regions.geometry[i].contains(p)][0]
    return average_risk(da, masks[ecoregion])


def query_by_shape(da, shape):
    mask = rm.mask_3D_geopandas(shape.simplify(0.002).buffer(0.005), da)[0]
    return average_risk(da, mask)


def query_by_location(da, lon, lat):
    latgrid = da['lat']
    longrid = da['lon']
    dist = (longrid - lon) ** 2 + (latgrid - lat) ** 2
    mask = dist < 0.5
    return average_risk(da, mask)


def project_risk(store='local', mode='ecoregion', lat=None, lon=None, id=None):
    if store == 'local':
        prefix = os.path.expanduser('~/workdir/carbonplan-data')

    if store == 'gs':
        prefix = 'gs://carbonplan-data'

    mapper = fsspec.get_mapper(prefix + '/processed/mtbs/conus/4000m/monthly.zarr')
    da = xr.open_zarr(mapper)['monthly']

    if mode == 'ecoregion':
        regions = gp.read_file(prefix + '/raw/ecoregions/supersections.geojson')
        risk = integrated_risk(query_by_ecoregion(da, lon, lat, regions))
        print(risk)

    if mode == 'shape':
        shape = gp.read_file(prefix + f'/raw/projects/{id}.json')
        risk = integrated_risk(query_by_shape(da, shape))
        print(risk)

    if mode == 'location':
        risk = integrated_risk(query_by_location(da, lon, lat))
        print(risk)


if __name__ == '__main__':
    fire.Fire(project_risk)
