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
