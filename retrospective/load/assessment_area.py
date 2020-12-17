import pandas as pd

from retrospective import utils

ASSESSMENT_AREA_CONFIGS = {
    "2011": {
        "io": "../data/ARB/2011_arb_assessment_file.xls",
        "names": [
            "supersection",
            "assessment_area",
            "species",
            "site_class",
            "board_feet",
            "basal_area",
            "common_practice",
            "diversity_index",
            "fire_risk",
            "rotation_length",
            "harvest_value",
        ],
        "sheet_name": 0,
        "skiprows": 1,
    },
    "2014": {
        "io": "../data/ARB/2014_arb_assessment_file.xls",
        "names": [
            "supersection",
            "assessment_area",
            "species",
            "site_class",
            "board_feet",
            "basal_area",
            "common_practice",
            "diversity_index",
            "fire_risk",
            "rotation_length",
            "harvest_value",
        ],
        "sheet_name": 0,
        "skiprows": 1,
    },
    "2015": {
        "io": "../data/ARB/2015_arb_assessment_file.xlsx",
        "names": [
            "supersection",
            "assessment_area",
            "species",
            "site_class",
            "basal_area",
            "common_practice",
            "diversity_index",
            "rotation_length",
            "harvest_value",
        ],
        "sheet_name": 1,
        "skiprows": 0,
    },
}


def load_assessment_areas(year=2015):
    """Load ARB official assessment area look up table.
    Each version has its own little differences that are handled here, including dealing with typos
    """
    df = pd.read_excel(**ASSESSMENT_AREA_CONFIGS[year]).fillna(
        method="ffill"
    )  # raw excel files has merged rows, ffill eliminates

    # TODO: has this typo existed since CAR days? Yup, it does.
    df["supersection"] = df["supersection"].str.replace("Mongollan", "Mongollon")

    df["assessment_area"] = df["assessment_area"].str.replace("Mongollan", "Mongollon")
    df["assessment_area"] = df["assessment_area"].str.replace(
        "MongollonOak Woodland", "Mongollon Oak Woodland"
    )
    df["assessment_area"] = df["assessment_area"].str.replace(
        "^Coast Redwood", "Northern California Coast Redwood"
    )
    df["assessment_area"] = df["assessment_area"].str.replace("AroostookHills", "Aroostook Hills")
    df["assessment_area"] = df["assessment_area"].str.replace("& Ontario Con", "& Lake Plains Con")
    df["assessment_area"] = df["assessment_area"].str.replace(
        "Aroostook-Maine-New Brunswick Hills", "Aroostook Hills"
    )

    # TODO: fix ton of species typos
    df["site_class"] = df['site_class'].map(
        {"All*": "all", "All": "all", "High": "high", "Low": "low"}
    )
    df["aa_code"] = df["assessment_area"].map(utils.load_aa_codes())
    df["ss_code"] = df["supersection"].map(utils.load_ss_codes())
    return df
