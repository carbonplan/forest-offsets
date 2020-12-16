import pandas as pd

from retrospective import utils


def preprocess_assessment_area_file(params):
    df = pd.read_excel(**params).fillna(
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


def main():
    protocol_configs = {
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

    for protocol_version, params in protocol_configs.items():
        lut = preprocess_assessment_area_file(params)
        lut.to_csv(f"../data/{protocol_version}_aa_lut.csv", index=False, float_format="%.2f")


if __name__ == "__main__":
    main()
