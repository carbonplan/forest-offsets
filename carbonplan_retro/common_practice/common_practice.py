import geopandas as gpd


def subset_common_practice_data(
    data: gpd.GeoDataFrame,
    spatial_filter: gpd.GeoDataFrame,
    temporal_filter: dict,
    fortyps: list,
    site_class: str,
    use_fldtypcd=False,
):
    """Subset full CP data by space, time, forest community, and site class.

    Common practice involves aggregating together underlying FIA plot/condition data.
    In all cases, there are four sets of criteria:
    - temporal: plots measured during a period of time
    - spatial: plots measured within some geographic range (e.g., supersection)
    - fortyp: plots of a certain "forest type"
    - site_class: plots of site_class: high, low, and all.
    """
    data = data[
        (data['MEASYEAR'] >= temporal_filter['min_year'])
        & (data['MEASYEAR'] <= temporal_filter['max_year'])
    ]

    if use_fldtypcd:
        forest_typ_var = 'FLDTYPCD'
    else:
        forest_typ_var = 'FORTYPCD'
    data = data[data[forest_typ_var].isin(fortyps)]

    site_class_ranges = {
        'high': {'max': 4, 'min': 1},
        'low': {'max': 7, 'min': 5},
        'all': {'max': 7, 'min': 1},
    }

    data = data[
        (data['SITECLCD'] <= site_class_ranges[site_class]['max'])
        & (data['SITECLCD'] >= site_class_ranges[site_class]['min'])
    ]
    data = gpd.sjoin(data, spatial_filter, how='inner', op='within')

    return data
