def get_mean_slag(data, fortypcds=None, site_class=None):
    site_class_ranges = {
        'high': {'max': 4, 'min': 1},
        'low': {'max': 7, 'min': 5},
        'all': {'max': 7, 'min': 1},
    }

    valid_site_classes = ['all', 'low', 'high']
    if site_class not in valid_site_classes:
        raise ValueError(f"site class must be in {[x for x in valid_site_classes]}")

    species_subset = data[data['FORTYPCD'].isin(fortypcds)]

    site_class_range = site_class_ranges[site_class]
    final_subset = species_subset[
        (species_subset['SITECLCD'] <= site_class_range['max'])
        & (species_subset['SITECLCD'] >= site_class_range['min'])
    ]

    return final_subset['slag_co2e_acre'].mean()


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


def get_arbocs(baseline_components, rp_components):
    """recalcualte arbocs as a function of predicted ifm1
    only ifm1 changes -- all other components are scaled to be proportional to observed <component>/ifm_1
    scale_components: experimented with whether other bits of the baseline needed to move with changes in IFM-1 -- do
    """

    baseline_carbon = baseline_components['ifm_1'] + baseline_components['ifm_3']
    onsite_carbon = rp_components['ifm_1'] + rp_components['ifm_3']
    adjusted_onsite = onsite_carbon * round(1 - rp_components['confidence_deduction'], 5)

    delta_onsite = adjusted_onsite - baseline_carbon

    baseline_wood_products = baseline_components['ifm_7'] + baseline_components['ifm_8']
    actual_wood_products = rp_components['ifm_7'] + rp_components['ifm_8']
    leakage_adjusted_delta_wood_products = (actual_wood_products - baseline_wood_products) * 0.8

    secondary_effects = rp_components['secondary_effects']
    secondary_effects = min(0, secondary_effects)  # Never allowed to have positive SE.

    calculated_allocation = delta_onsite + leakage_adjusted_delta_wood_products + secondary_effects
    return calculated_allocation


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
