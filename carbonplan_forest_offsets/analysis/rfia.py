# given a classificaiton dict, spit out some common practice values
from collections import Counter
from functools import lru_cache

import numpy as np
import pandas as pd

from ..data import cat
from ..utils import aa_code_to_ss_code


@lru_cache(maxsize=None)
def load_rfia_data(assessment_area_id, site_class='all'):

    valid_site_classes = ['all', 'low', 'high']
    if site_class not in valid_site_classes:
        raise ValueError(f"site class must be in {[x for x in valid_site_classes]}")

    keys = [
        'YEAR',
        'FORTYPCD',
        'site',
        'CARB_ACRE',
        'CARB_TOTAL',
        'AREA_TOTAL',
        'CARB_ACRE_VAR',
        'CARB_TOTAL_VAR',
        'AREA_TOTAL_VAR',
        'nPlots_TREE',
        'nPlots_AREA',
    ]

    if site_class == 'all':
        data = cat.rfia_all(assessment_area_id=int(assessment_area_id)).read()
        data['site'] = 'all'
        data = data[keys]
    else:
        data = cat.rfia(assessment_area_id=int(assessment_area_id)).read()[keys]
        data = data[data['site'] == site_class]

    data = data[data['CARB_TOTAL'] > 0]
    data = data[data['YEAR'] >= 2010]  # excluded in attempt to speed up uncertainty propogation

    return data


def calculate_fortyp_weighted_slag(data, weights):
    percent_null = sum(
        [weight for fortyp, weight in weights.items() if fortyp not in data['FORTYPCD'].values]
    )
    return pd.Series(
        (np.nansum(data['FORTYPCD'].map(weights) * data['common_practice']), percent_null)
    )


def get_rfia_slag_co2e_acre(data, uncertainty=False):

    if uncertainty:
        carbon_uncertainty = np.random.normal(0, 1, [len(data)]) * (data['CARB_ACRE_VAR'] ** 0.5)
        data['CARB_ACRE'] = data['CARB_ACRE'] + carbon_uncertainty
        return data['CARB_ACRE'] * 44 / 12 * 0.907185

    sums = data.sum()

    # Could maybe revisit handling of low sample sizes at some point?
    # if sums['nPlots_TREE'] < 5: # minimum number of plots wi aggregation needed to yield estimate

    return (
        sums['CARB_TOTAL'] / sums['AREA_TOTAL'] * 44 / 12 * 0.907185
    )  # 44/12 converts carbon to co2; 0.907 takes us from US short to tonnes


def summarize_project_by_ss(project, supersection_id):
    '''For reclassification of 999, need to know fraction of area per superseciton and site class'''
    site_class_acreage = Counter()
    for assessment_area in project['assessment_areas']:
        if aa_code_to_ss_code().get(assessment_area['code']) == supersection_id:
            site_class_acreage[assessment_area['site_class']] += assessment_area['acreage']

    return [
        {'code': supersection_id, 'site_class': site_class, 'acreage': acreage}
        for site_class, acreage in site_class_acreage.items()
    ]


def get_rfia_arb_common_practice(project, use_site_class=None):
    """Recalculate ARB CP using underlying rFIA-processed data

    use_site_class: fixes site_class to constant as opposed to using per-assessment-area value
    """
    store = []
    for assessment_area in project['assessment_areas']:
        if assessment_area['code'] == 999:
            # 999 only used for reclassification -- skip it.
            continue

        if use_site_class:
            rfia_data = load_rfia_data(assessment_area['code'], site_class=use_site_class)
        else:
            rfia_data = load_rfia_data(
                assessment_area['code'], site_class=assessment_area['site_class']
            )

        cp_per_inventory = rfia_data.groupby('YEAR').apply(get_rfia_slag_co2e_acre)

        median_cp = cp_per_inventory.loc[
            (cp_per_inventory.index <= 2013) & (cp_per_inventory.index >= 2010)
        ].median()

        if np.isnan(median_cp):
            # this only occurs when cross-state evals cannot be matched historically. Northern ID/OR/WA is on ex.
            median_cp = cp_per_inventory.loc[(cp_per_inventory.index >= 2010)].median()

        weighted_cp = median_cp * assessment_area['acreage'] / project['acreage']
        store.append(weighted_cp)
    cp = sum(store)
    return cp


def get_fortyp_weighted_slag_co2e_acre(
    supersection_id, fortyp_weights, site_class, uncertainty=False
):
    '''calculate SLAG (CO2e per acre) within a superseciton, weighting by forest types'''
    # find all assessment areas per supersection, in the event that classifier doesnt like the aa assignment developer has choosen
    assessment_area_ids = [
        aa_id for aa_id, ss_id in aa_code_to_ss_code().items() if ss_id == supersection_id
    ]

    rfia_data = pd.concat(
        [
            load_rfia_data(assessment_area_id, site_class=site_class)
            for assessment_area_id in assessment_area_ids
        ]
    )

    common_practice_by_fortyp = (
        rfia_data.groupby(['YEAR', 'FORTYPCD'])
        .apply(get_rfia_slag_co2e_acre, (uncertainty))
        .rename('common_practice')
        .reset_index(level=['FORTYPCD'])
    )

    df = (
        common_practice_by_fortyp.groupby('YEAR')
        .apply(calculate_fortyp_weighted_slag, (fortyp_weights))
        .rename(columns={0: 'fortyp_cp', 1: 'percent_null'})
    )

    median_slag = df.loc[(df.index <= 2013) & (df.index >= 2010), 'fortyp_cp'].median()

    if df.loc[(df.index <= 2013) & (df.index >= 2010), 'percent_null'].max() > 0.2:
        return np.nan

    # if np.isnan(median_slag):
    #    median_slag = df.loc[(df.index == 2010), 'fortyp_cp'].median()

    if np.isnan(median_slag):
        # this only occurs when cross-state evals cannot be matched historically.
        median_slag = df.loc[(df.index >= 2010), 'fortyp_cp'].median()
        if df.loc[(df.index >= 2010), 'percent_null'].max() > 0.2:
            return np.nan

    return median_slag


def contains_999_aa(project):
    '''check if project has a 999 assessment area, which requires special handling due to missingness of species data in assessment area'''
    all_species = [
        assessment_area
        for assessment_area in project['assessment_areas']
        if assessment_area['code'] == 999
    ]
    if len(all_species) > 0:
        return True
    else:
        return False


def get_project_weighted_slag(
    project, project_classification, use_site_class=None, uncertainty=False
):
    store = []
    for k, fortyp_probas in project_classification.items():
        classification_ss_id, classification_aa_id = eval(
            k
        )  # json keys are string so gotta coerce back to usable values.
        fortyp_probas = {
            float(k): v for k, v in fortyp_probas.items()
        }  # convert fortyp str to float; this is just more json-specific re-casting

        if contains_999_aa(project):
            return get_project_weighted_slag_no_species(
                project,
                project_classification,
                use_site_class=use_site_class,
                uncertainty=uncertainty,
            )

        # assessment areas can show up twice -- once for high and once for low site class. so we've got to loop through assessment areas now.
        for assessment_area in project['assessment_areas']:
            if assessment_area['code'] == classification_aa_id:
                if classification_aa_id == 999:
                    raise ValueError(
                        'Project doesnt have species per assessment area -- need to run through alternate method'
                    )

                if use_site_class:
                    median_cp = get_fortyp_weighted_slag_co2e_acre(
                        classification_ss_id, fortyp_probas, use_site_class, uncertainty=uncertainty
                    )
                else:
                    median_cp = get_fortyp_weighted_slag_co2e_acre(
                        classification_ss_id,
                        fortyp_probas,
                        assessment_area['site_class'],
                        uncertainty=uncertainty,
                    )

                if np.isnan(median_cp):
                    print(f'{project["opr_id"]} has issues w classification lookup')
                weighted_cp = median_cp * assessment_area['acreage'] / project['acreage']
                # print(assessment_area['code'], median_cp)
                store.append(weighted_cp)
    cp = sum(store)
    return cp


def get_project_weighted_slag_no_species(
    project, project_classification, use_site_class=None, uncertainty=False
):
    store = []

    for k, fortyp_probas in project_classification.items():
        classification_ss_id, classification_aa_id = eval(
            k
        )  # json keys are string so gotta coerce back to usable values.
        fortyp_probas = {float(k): v for k, v in fortyp_probas.items()}

        psuedo_assessment_areas = summarize_project_by_ss(project, classification_ss_id)

        for assessment_area in psuedo_assessment_areas:
            if use_site_class:
                median_cp = get_fortyp_weighted_slag_co2e_acre(
                    classification_ss_id, fortyp_probas, use_site_class, uncertainty=uncertainty
                )
            else:
                median_cp = get_fortyp_weighted_slag_co2e_acre(
                    classification_ss_id,
                    fortyp_probas,
                    assessment_area['site_class'],
                    uncertainty=uncertainty,
                )

            weighted_cp = median_cp * assessment_area['acreage'] / project['acreage']
            # print(assessment_area['code'], median_cp, assessment_area['acreage']/project['acreage'])

            store.append(weighted_cp)

    return sum(store)
