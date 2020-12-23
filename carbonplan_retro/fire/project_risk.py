import fire
import regionmask
import xarray as xr
from scipy.stats import binom
from shapely.geometry import Point

from ..data import cat


def integrated_risk(p: float):
    """Calculate the integrated 100-yr risk given an annualized risk

    Parameters
    ----------
    p : float
        Annualized risk probability

    Returns
    -------
    float
        Integrated 100-yr riskå
    """
    return 1 - binom.cdf(0, 100, p)


def average_risk(da: xr.DataArray, mask: xr.DataArray) -> xr.DataArray:
    """Calculate the average fire risk over a masked region

    Parameters
    ----------
    da : xr.DataArray
        Fractional area burned array with dims: (time, y, x)
    mask : xr.DataArray
        Mask to subset `da` before calculating the average fire risk, dims: (region, y, x)

    Returns
    -------
    float
        Average (observed) burn risk aggregated over regions
    """
    return (
        da.where(mask)
        .mean(['x', 'y'])
        .groupby('time.year')
        .sum()
        .clip(0, 1)
        .sel(year=slice('2001', '2018'))
        .mean()
    )


def query_by_region(da, lon, lat, regions):
    p = Point(lon, lat)
    masks = regionmask.mask_3D_geopandas(regions, da, drop=False)
    region = [i for i in regions.index if regions.geometry[i].contains(p)][0]
    return average_risk(da, masks[region])


def query_by_shape(da, shape, simplify=0.002, buffer=0.005):
    mask = regionmask.mask_3D_geopandas(shape.simplify(simplify).buffer(buffer), da)[0]
    return average_risk(da, mask)


def query_by_location(da, lon, lat):
    latgrid = da['lat']
    longrid = da['lon']
    dist = (longrid - lon) ** 2 + (latgrid - lat) ** 2
    mask = dist < 0.5
    return average_risk(da, mask)


def project_risk(mode='supersection', lat=None, lon=None, id=None):

    da = cat.mtbs.to_dask()

    if mode in ['supersection', 'baileys', 'location'] and (lat is None or lon is None):
        raise ValueError(f'{mode} mode requires lat/lon parameters')
    if mode == 'shape' and id is None:
        raise ValueError('shape mode requires id parameter')

    if mode == 'supersection':
        regions = cat.supersections.read()
        risk = query_by_region(da, lon, lat, regions)
    elif mode == 'baileys':
        regions = cat.baileys_ecoregions.read()
        risk = query_by_region(da, lon, lat, regions)
    elif mode == 'shape':
        shape = cat.arb_geometries(opr_id=id).read()
        risk = query_by_shape(da, shape)
    elif mode == 'location':
        risk = query_by_location(da, lon, lat)
    else:
        raise ValueError(f'invalid mode: {mode}')

    return integrated_risk(risk)


if __name__ == '__main__':
    fire.Fire(project_risk)
