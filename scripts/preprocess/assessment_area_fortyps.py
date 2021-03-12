import json
import os

import fsspec

from carbonplan_retro.data import cat
from carbonplan_retro.utils import assessment_area_str_to_aa_code


def get_fortyp_mapping():
    """ARB repo"""
    df = cat.arb_tabling_csv.read()

    # deal w typos between assessment area data file that based indexing on.
    df["Community"] = df["Community"].str.replace("Andirondacks", "Adirondacks")

    df['Community'] = df['Community'].str.replace(',', '')
    df["Community"] = df["Community"].str.replace("MongollonOak Woodland", "Mongollon Oak Woodland")
    df["Community"] = df["Community"].str.replace("Coast Coast", "Coast")
    df["Community"] = df["Community"].str.replace(
        "Northern California Coast Mixed Oak Woodland", "Mixed Oak Woodland"
    )

    df['fortypcds'] = (
        df['rest'].str.extract(r'\(([\d,\s]+)\)')[0].apply(lambda x: [int(y) for y in x.split(',')])
    )
    df['aa_code'] = df['Community'].map(assessment_area_str_to_aa_code())
    df = df[~df.duplicated(subset=['aa_code'])]

    return df.set_index('aa_code')['fortypcds'].to_dict()


if __name__ == '__main__':
    fortypcd_d = get_fortyp_mapping()
    with fsspec.open(
        'az://carbonplan-retro/ancillary/arb_fortypcds.json',
        account_key=os.environ["BLOB_ACCOUNT_KEY"],
        account_name="carbonplan",
        mode='w',
    ) as f:
        json.dump(fortypcd_d, f, indent=2)
