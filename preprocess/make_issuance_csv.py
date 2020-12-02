import pandas as pd


def process_issuance_table(fname):
    df = pd.read_excel(fname, sheet_name="ARB Offset Credit Issuance")
    # df = df[df['Project Type'] == 'Forest']

    rename_d = {
        "OPR Project ID": "proj_id",
    }
    df = df.rename(columns=rename_d)
    df['arb_id'] = df['CARB Issuance ID'].str[:8]

    return df


if __name__ == '__main__':
    fname = '~/forest-retro/ancillary-files/arboc_issuance_2020-09-09.xlsx'
    df = process_issuance_table(fname)
    df.to_csv('../data/issuance.csv', float_format='%.2f', index=False)
