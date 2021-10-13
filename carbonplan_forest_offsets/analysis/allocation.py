import pandas as pd


def calculate_allocation(
    data: pd.DataFrame, rp: int = 1, round_intermediates: bool = False
) -> pd.Series:
    """Calculate the allocation of ARBOCs based on project report IFM components

    Parameters
    ----------
    data : pd.DataFrame
        Project database
    rp : int
        Reporting period
    round_intermediates : bool
        If true, round intermeiate calculations before calculating the final allocation

    Returns
    -------
    allocation : pd.Series
    """

    baseline_carbon = (
        data['baseline']['components']['ifm_1'] + data['baseline']['components']['ifm_3']
    )

    onsite_carbon = (
        data[f'rp_{rp}']['components']['ifm_1'] + data[f'rp_{rp}']['components']['ifm_3']
    )
    adjusted_onsite = onsite_carbon * (1 - data[f'rp_{rp}']['confidence_deduction']).astype(
        float
    ).round(5)
    if round_intermediates:
        adjusted_onsite = adjusted_onsite.round()

    delta_onsite = adjusted_onsite - baseline_carbon

    baseline_wood_products = (
        data['baseline']['components']['ifm_7'] + data['baseline']['components']['ifm_8']
    )

    actual_wood_products = (
        data[f'rp_{rp}']['components']['ifm_7'] + data[f'rp_{rp}']['components']['ifm_8']
    )

    leakage_adjusted_delta_wood_products = (actual_wood_products - baseline_wood_products) * 0.8

    secondary_effects = data[f'rp_{rp}']['secondary_effects']
    secondary_effects[secondary_effects > 0] = 0  # Never allowed to have positive SE.

    if round_intermediates:
        leakage_adjusted_delta_wood_products = leakage_adjusted_delta_wood_products.round()

    calculated_allocation = delta_onsite + leakage_adjusted_delta_wood_products + secondary_effects
    calculated_allocation.name = 'opdr_calculated'
    return calculated_allocation


def get_rp1_arbocs(opr_id, baseline_components, rp_components):
    """Handles three projects where project harvest > baseline harvest
    Systematically handling this edge case would require additional data entry of harvest volumes per RP.

    Here, we get around those data limitations by codifying logic we have confirmed manuallly. This approach
    will not scale to subsequent reporting periods.
    """

    # Equation C.8 of 2015 protocol: if project harvest > baseline harvest, exclude landfill
    if opr_id in ['CAR1217', 'ACR247', 'ACR276']:
        baseline_components['ifm_8'] = 0
        rp_components['ifm_8'] = 0

    arbocs = get_arbocs(baseline_components, rp_components)
    return arbocs


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
