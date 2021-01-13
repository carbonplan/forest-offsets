import geopandas as gpd


def spatial_subset(
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


def geographic_id_subset(
    data: gpd.GeoDataFrame,
    geographic_id: int,
    temporal_filter: dict,
    fortyps: list,
    site_class: str,
    geographic_var: str = 'supersection_id',
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
    data = data[data[geographic_var] == geographic_id]

    return data


def predict_ifm_1_from_cp(slag, acreage, bounds=False):
    """given a value for slag (often CP) and acreage, calculate IFM-1
    scaling parameter derived from analysis in notebook/0x_recalculate-arbocs
    model is defined as ifm_1 ~ B1*(slag*acreage)

    values here reported when fit model for
    """
    mean = slag * acreage * 1.240944
    if bounds:
        low = slag * acreage * 1.235
        high = slag * acreage * 1.247
        return (low, mean, high)
    else:
        return mean


def get_arbocs_from_ifm1(new_ifm1, project_db, name='new_allocation', scale_components=False):
    """recalcualte arbocs as a function of predicted ifm1
    only ifm1 changes -- all other components are scaled to be proportional to observed <component>/ifm_1
    scale_components: experimented with whether other bits of the baseline needed to move with changes in IFM-1 -- do
    """

    alt_baseline_carbon = new_ifm1 + project_db['baseline']['components']['ifm_3']

    onsite_carbon = (
        project_db['rp_1']['components']['ifm_1'] + project_db['rp_1']['components']['ifm_3']
    )

    adjusted_onsite = onsite_carbon * (1 - project_db['rp_1']['confidence_deduction']).astype(
        float
    ).round(5)

    delta_onsite = adjusted_onsite - alt_baseline_carbon

    baseline_wood_products = (
        project_db['baseline']['components']['ifm_7']
        + project_db['baseline']['components']['ifm_8']
    )

    actual_wood_products = (
        project_db['rp_1']['components']['ifm_7'] + project_db['rp_1']['components']['ifm_8']
    )

    leakage_adjusted_delta_wood_products = (actual_wood_products - baseline_wood_products) * 0.8

    secondary_effects = project_db['rp_1']['secondary_effects']
    secondary_effects[secondary_effects > 0] = 0  # Never allowed to have positive SE.

    calculated_allocation = delta_onsite + leakage_adjusted_delta_wood_products + secondary_effects
    return calculated_allocation.rename(name)
