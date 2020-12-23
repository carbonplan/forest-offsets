import pandas as pd

from .. import utils


def load_olaf_common_practice() -> pd.DataFrame:
    """
    Load mdb-dump output of Olaf's output.accdb that retreived from ARB website
    """
    olaf_cp = pd.read_csv('../data/olaf/olaf-arb-fia-output.csv')
    site_class_mapper = {
        ' site class 1-4': 'high',
        ' site class 5-7': 'low',
        ' site class 1-7': 'all',
    }

    olaf_cp['assessment_area'] = olaf_cp.row_name.str[:-16]
    olaf_cp['raw_site_class'] = olaf_cp.row_name.str.split(',').apply(lambda x: x[-1])
    olaf_cp['site_class'] = olaf_cp['raw_site_class'].map(site_class_mapper)

    col_names = {'CO2 metric tons': 'slag_co2e_acre', 'Std CO2 metric tons': 'slag_co2e_std'}
    olaf_cp = olaf_cp.rename(columns=col_names)
    return olaf_cp


def load_olaf_df() -> pd.DataFrame:
    """Loading Olaf's CP and "tabling" sheet.
    Tabling data found on ARB website in FIA data dump. Contains intermediary data on
    which FORTYPCDs map to each assessment area -- as well as sum of cond_prop_group which
    tells us something about the number of CONDS used to estimate CP
    """
    # load CP data because it has same typos -- if we join at the outset, only have to correct typos once
    olaf_cp = load_olaf_common_practice()

    olaf_tabling = pd.read_excel('../data/olaf/tabling_rest_supersection.xlsx')
    olaf_tabling = olaf_tabling.rename(
        columns={
            'site': 'site_class',
            'Supersection_Name': 'supersection',
            'Community': 'assessment_area',
        }
    )

    df = olaf_tabling.join(
        olaf_cp.set_index(['assessment_area', 'site_class']), on=['assessment_area', 'site_class']
    )

    df["supersection"] = df["supersection"].str.replace("Andirondacks", "Adirondacks")

    df["assessment_area"] = df["assessment_area"].str.replace("Andirondacks", "Adirondacks")
    df['supersection'] = df['supersection'].str.replace(
        'Ouachita Mixed Forest-Meadow Ouachita Mountains', 'Ouachita Mixed Forest'
    )
    df['assessment_area'] = df['assessment_area'].str.replace(',', '')
    df["assessment_area"] = df["assessment_area"].str.replace(
        "MongollonOak Woodland", "Mongollon Oak Woodland"
    )
    df["assessment_area"] = df["assessment_area"].str.replace("Coast Coast", "Coast")
    df["assessment_area"] = df["assessment_area"].str.replace(
        "Northern California Coast Mixed Oak Woodland", "Mixed Oak Woodland"
    )

    df["aa_code"] = df["assessment_area"].map(utils.load_aa_codes())
    df["ss_code"] = df["supersection"].map(utils.load_ss_codes())
    return df
