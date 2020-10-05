import pandas as pd

protocol_configs = {
    "2011": {
        "io": "data/ARB/2011_arb_assessment_file.xls",
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
        "io": "data/ARB/2014_arb_assessment_file.xls",
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
        "io": "data/ARB/2015_arb_assessment_file.xlsx",
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


def ss_aa_key(data):
    return data["supersection"] + "_" + data["assessment_area"]


protocols = {}
for protocol_version, params in protocol_configs.items():
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
    df["assessment_area"] = df["assessment_area"].str.replace(
        "AroostookHills", "Aroostook Hills"
    )
    df["assessment_area"] = df["assessment_area"].str.replace(
        "& Ontario Con", "& Lake Plains Con"
    )
    df["assessment_area"] = df["assessment_area"].str.replace(
        "Aroostook-Maine-New Brunswick Hills", "Aroostook Hills"
    )

    # TODO: fix ton of species typosx`

    df["ss_aa"] = ss_aa_key(df)

    protocols[protocol_version] = df


current_ss_aa = protocols["2015"]["ss_aa"].unique().tolist()
mid_ss_aa = protocols["2014"]["ss_aa"].unique().tolist()
og_ss_aa = protocols["2011"]["ss_aa"].unique().tolist()


ser = (protocols["2011"]["species"].str.replace("/", ",")).str.split(",")
all_species = list(
    set(
        [
            species.lower().strip()
            for species_list in ser.to_list()
            for species in species_list
        ]
    )
)

#  https://stackoverflow.com/questions/13636848/is-it-possible-to-do-fuzzy-match-merge-with-python-pandas
from fuzzywuzzy import process

all_fia_names = fia["COMMON_NAME"].tolist()
d = {}
for spp in all_species:
    matches = process.extract(spp, all_fia_names, limit=1)
    for match in matches:
        fia_spp_name, conf = match
        if conf > 80:
            fia_code = fia[fia["COMMON_NAME"] == fia_spp_name].iloc[0]["FHM_SPECIES"]
        d[spp] = fia_code
