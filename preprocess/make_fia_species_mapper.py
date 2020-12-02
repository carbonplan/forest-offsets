from glob import glob
import json

from fuzzywuzzy import process
import numpy as np
import pandas as pd


def generate_assessment_fia_species_map():
    fia = pd.read_excel(
        "/Users/darryl/proj/carbonplan/retro/data/FHM_Species_codes.xls", skiprows=1
    )
    all_fia_names = fia.loc[np.isnan(fia["YEAR_DROPPED"]), "COMMON_NAME"].tolist()

    lut_fnames = glob("../data/*_aa_lut.csv")
    aa_lut = pd.concat([pd.read_csv(lut_fname) for lut_fname in lut_fnames])

    aa_species_lst = (
        (aa_lut["species"].str.replace("/", ",")).str.split(",").tolist()
    )  # list of lists.

    full_species_lst = list(
        set([species.lower().strip() for species_lst in aa_species_lst for species in species_lst])
    )

    spp_name_to_fia_code = {}
    for spp in full_species_lst:
        matches = process.extract(spp, all_fia_names, limit=1)
        for match in matches:
            fia_spp_name, conf = match
            if conf > 80:
                fia_code = fia[fia["COMMON_NAME"] == fia_spp_name].iloc[0]["FHM_SPECIES"]

                # `.item()` casts from numpy.int to python int
                spp_name_to_fia_code[spp] = fia_code.item()

    return spp_name_to_fia_code


if __name__ == "__main__":
    d = generate_assessment_fia_species_map()
    with open('../data/species_name_to_fia_code.json', 'w') as f:
        json.dump(d, f)
