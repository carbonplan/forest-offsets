import numpy as np
import pandas as pd

from ..data import cat

ifm_opr_ids = [
    'CAR1013',
    'CAR1217',
    'ACR210',
    'ACR288',
    'CAR973',
    'ACR423',
    'ACR281',
    'ACR211',
    'VCSOPR10',
    'CAR1041',
    'ACR273',
    'ACR425',
    'CAR1046',
    'ACR280',
    'ACR274',
    'CAR1070',
    'CAR1102',
    'CAR1134',
    'CAR1160',
    'CAR1103',
    'CAR1104',
    'CAR1161',
    'ACR192',
    'CAR1159',
    'ACR371',
    'CAR1175',
    'ACR378',
    'CAR1314',
    'ACR173',
    'CAR1173',
    'CAR1180',
    'CAR1174',
    'ACR189',
    'ACR377',
    'ACR324',
    'ACR267',
    'ACR260',
    'CAR1099',
    'ACR256',
    'CAR1063',
    'CAR1205',
    'ACR202',
    'CAR1062',
    'ACR257',
    'CAR993',
    'CAR1257',
    'CAR1098',
    'ACR292',
    'CAR1204',
    'CAR1086',
    'ACR247',
    'CAR1088',
    'ACR427',
    'ACR249',
    'ACR276',
    'ACR282',
    'CAR1213',
    'ACR248',
    'ACR284',
    'ACR279',
    'ACR417',
    'CAR1215',
    'CAR1197',
    'CAR1190',
    'ACR199',
    'ACR360',
    'ACR393',
    'CAR1139',
    'CAR1130',
    'ACR361',
    'CAR1191',
    'CAR1162',
    'ACR303',
    'CAR1100',
    'CAR1140',
    'CAR1147',
    'ACR182',
    'CAR1183',
    'ACR373',
    'CAR1141',
    'CAR1208',
    'CAR1297',
    'CAR1264',
    'CAR1094',
    'ACR255',
    'CAR1067',
    'CAR1209',
    'ACR458',
    'CAR1032',
    'ACR200',
    'CAR1066',
    'CAR1095',
    'ACR262',
]


def load_issuance_table(forest_only: bool = True) -> pd.DataFrame:
    """Load and clean ARB issuance table.

    Parameters
    ----------

    forest_only : bool, optional
        Return only forest projects.

    Returns
    -------
    df : pandas.DataFrame
        Issuance table returned as a Pandas.DataFrame
    """
    df = cat.issuance_table.read()

    rename_d = {
        'OPR Project ID': 'opr_id',
        'ARB Offset Credits Issued': 'allocation',
        'Project Type': 'project_type',
        'Issuance Date': 'issued_at',
        'Forest Buffer Account Contribution': 'buffer_pool',
        'Reporting Period Start Date': 'rp_start_at',
        'Reporting Period End Date': 'rp_end_at',
        'Vintage': 'vintage',
    }
    df = df.rename(columns=rename_d)

    df['project_type'] = df['project_type'].str.lower()

    # can be multiple issuance in single RP -- grab issuance ID so can aggregate later
    df['arb_rp_id'] = df['CARB Issuance ID'].str[9]
    df['arb_id'] = df['CARB Issuance ID'].str[:8]

    df['is_ea'] = df['Early Action/ Compliance'] == 'EA'

    # fill reforest defer as nan
    df = df.replace("reforest defer", np.nan)

    new_order = [
        'project_type',
        'opr_id',
        'arb_id',
        'is_ea',
        'arb_rp_id',
        'rp_start_at',
        'rp_end_at',
        'vintage',
        'issued_at',
        'allocation',
        'buffer_pool',
        'CARB Issuance ID',
        'Early Action/ Compliance',
        'Invalidation Timeframe',
        'Date Invalidation Period Reduced',
        'Start of Invalidation Timeframe',
        'Offset Project Name',
        'Offset Project Operator',
        'Verification Body',
        'State',
        'Provides DEBS',
        'Section 95989(b) Documentation, if applicable',
        'Project Documentation',
        'Retired Voluntarily',
        'Retired 1st Compliance Period (CA)',
        'Retired 2nd Compliance Period (CA)',
        'Retired 3rd Compliance Period (CA)',
        'Retired for Compliance in Quebec',
        'Comment',
    ]

    df = df[new_order]

    if forest_only:
        return df[df['project_type'] == 'forest']
    else:
        return df


def get_arb_id_map() -> dict:
    df = load_issuance_table(forest_only=False)[['opr_id', 'arb_id']].set_index('arb_id')
    return df.opr_id.to_dict()
