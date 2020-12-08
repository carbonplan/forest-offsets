import pandas as pd

ISSUANCE_URL = 'https://ww3.arb.ca.gov/cc/capandtrade/offsets/issuance/arboc_issuance.xlsx'


def issuance(fn=ISSUANCE_URL, forest_only=True):
    """Load and clean ARB issuance table.
    Default is to pull a clean sheet from ARB website -- but can also specify fn.
    """
    df = pd.read_excel(fn, sheet_name='ARB Offset Credit Issuance')

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
