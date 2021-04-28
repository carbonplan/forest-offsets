import json

import fsspec
import numpy as np

from carbonplan_forest_offsets.data import cat, get_retro_bucket
from carbonplan_forest_offsets.load.issuance import ifm_opr_ids, load_issuance_table


def subset_stats(df, criteria):
    nunique_opr_ids = df[criteria]["opr_id"].nunique()
    total_arbocs = df[criteria]["allocation"].sum()
    return (nunique_opr_ids, total_arbocs)


def main():
    df = load_issuance_table(forest_only=False)

    project_db = cat.project_db_json.read()

    upfront_opr_ids = [
        project["opr_id"]
        for project in project_db
        if project["carbon"]["initial_carbon_stock"]["value"]
        > project["carbon"]["common_practice"]["value"]
    ]

    non_graduated_opr_ids = [project["opr_id"] for project in project_db]

    subsets = {
        "all": np.tile(True, len(df)),
        "all_forest": df["project_type"] == "forest",
        "compliance_ifm": (df["opr_id"].isin(ifm_opr_ids))
        & (df["Early Action/ Compliance"] == "COP"),
        "non_graduated_compliance_ifms": (df["opr_id"].isin(non_graduated_opr_ids))
        & (df["Early Action/ Compliance"] == "COP"),
        "upfront_ifm": (df["opr_id"].isin(upfront_opr_ids)) & (df["arb_rp_id"].isin(["A"])),
    }

    summary = {k: subset_stats(df, v) for k, v in subsets.items()}
    return summary


if __name__ == '__main__':
    summary = main()

    fs_prefix, fs_kwargs = get_retro_bucket()
    fn = f"{fs_prefix}/results/issuance-stats.json"
    with fsspec.open(fn, mode='w', **fs_kwargs) as f:
        json.dump(summary, f, indent=2)
