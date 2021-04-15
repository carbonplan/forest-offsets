#!/usr/bin/env python

# Build Retro-DB and Retro-DB-light databases
#
# This script munges a collection of datasets to create what we refer to as the `Project-DB`.
#
# The source datasets include:
#
# - Project-DB: A Google sheet including the digitized forest carbon offset project details.
# - ARB Issuance table: A spreadsheet including the official ARBOC issuances (version dated September
#   9, 2020).
# - Project shapes: A collection of GeoJSON datasets describing the boundaries of each project in
#   Project-DB.
#
# In addition to the static datasets above, we derive the following datasets:
#
# - OPDR-Calculated: issuance derived from IFM-1, IFM-3, IFM-7, IFM-8, and secondary effects (SE).
#
# Like all munging workflows, not everything here is perfectly polished, but the resulting datasets
# are clean and should be useful for holistic analysis of the programs included.

# At the end of this script, we end up with three "allocation" values:

# - Issuance: the official, issued allocation of ARBOCs as recorded by ARB.
# - OPDR-Reported: the OPO/APD reported ARBC
# - OPDR-Calculated: issuance derived from IFM-1, IFM-3, IFM-7, IFM-8, and secondary effects (SE).

import json
import os
import shutil

import click
import geopandas
import pandas as pd
import shapely
from tqdm import tqdm

from carbonplan_forest_offsets.analysis import allocation
from carbonplan_forest_offsets.data import get_retro_bucket
from carbonplan_forest_offsets.load.issuance import load_issuance_table
from carbonplan_forest_offsets.load.project_db import load_project_db

DB_VERSION = 'v1.0'
script_dir = os.path.dirname(os.path.realpath(__file__))


def write_shapes(opr_ids, project_dir, target_dir):

    print('writing shapes')
    for i, proj in tqdm(enumerate(opr_ids)):
        src = f"{project_dir}/shapes/{proj}.json"
        dst = f"{target_dir}/projects/{proj}"
        dst_file = os.path.join(dst, "shape.json")
        os.makedirs(dst, exist_ok=True)

        with open(src) as f:
            data = json.load(f)

        if len(data["features"]) == 1:
            data["features"][0]["properties"] = {"id": proj}
        else:
            gdf = geopandas.GeoDataFrame.from_file(src)
            data = json.loads(geopandas.GeoDataFrame(geometry=[gdf.unary_union]).to_json())
            data["features"][0]["properties"] = {"id": proj}

        with open(dst_file, "w") as f:
            json.dump(data, f, indent=2, allow_nan=False)


def write_opdrs(opr_ids, project_dir, target_dir):
    print('writing opdrs')
    for i, proj in tqdm(enumerate(opr_ids)):
        src = f"{project_dir}/carbonplan-retro/packaged_opdrs/{proj}.zip"
        dst = f"{target_dir}/projects/{proj}"
        dst_file = os.path.join(dst, "opdrs.zip")
        os.makedirs(dst, exist_ok=True)
        shutil.copyfile(src, dst_file)


def write_ancillary_files(project_dir, target_dir):
    dst = f'{target_dir}/ancillary/'
    for fname in [
        "arboc_issuance_2020-09-09.xlsx",
        "assessment_area_lookup.csv",
        "super_section_lookup.csv",
    ]:
        src = f"{project_dir}/carbonplan-retro/ancillary/{fname}"
        dst_file = os.path.join(dst, fname)
        os.makedirs(dst, exist_ok=True)
        shutil.copyfile(src, dst_file)


def copy_doc_files(target_dir):
    for fname in ["README.md", "forest-offsets-database-schema-v1.0.json", "glossary.md"]:
        src = f"{script_dir}/{fname}"
        dst = f"{target_dir}/{fname}"
        shutil.copyfile(src, dst)


def get_centroids(gdf):
    crs = "+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs"
    geom = gdf.to_crs(crs).simplify(8000).buffer(8000).to_crs("lonlat").geometry.item()

    if isinstance(geom, shapely.geometry.multipolygon.MultiPolygon):
        areas = [g.area for g in geom]
        centroids = [[g.centroid.x, g.centroid.y] for g in geom]
        # sort by area (largest first)
        centroids = [x for _, x in sorted(zip(areas, centroids), reverse=True)]
    elif isinstance(geom, shapely.geometry.polygon.Polygon):
        centroids = [[geom.centroid.x, geom.centroid.y]]
    else:
        raise ValueError("geom was not a polygon/multipolygon: %s" % type(geom))
    return centroids


def make_rp_1(row):
    d = {}
    try:
        d["start_date"] = row["rp_1", "start", ""].strftime("%Y-%m-%d")
    except ValueError:
        d["start_date"] = None

    try:
        d["end_date"] = row["rp_1", "end", ""].strftime("%Y-%m-%d")
    except ValueError:
        d["end_date"] = None

    d.update(row["rp_1"]["components"][["ifm_1", "ifm_3", "ifm_7", "ifm_8"]].to_dict())
    d["secondary_effects"] = row["rp_1", "secondary_effects", ""]
    d["confidence_deduction"] = row["rp_1", "confidence_deduction", ""]

    return d


def valid_species(species):

    for c in species:
        if "basal_area" not in c:
            print("setting missing basal_area to None")
            c["basal_area"] = None
        if "fraction" not in c:
            print("setting missing fraction to None")
            c["fraction"] = None
    return species


def make_supersections(row):
    species = row["project", "species", ""]
    if not isinstance(species, dict):
        print("species was not a dict")
        species = None
    else:
        species = species.copy()

    data = row["project", "assessment_areas", ""]

    if not data:
        print("assessment_areas was empty or null")
        return []
    else:
        data = data.copy()

    for aa in data:
        if species:
            if str(aa["code"]) in species:
                aa["species"] = valid_species(species[str(aa["code"])])
            elif "all" in species:
                aa["species"] = []
            else:
                print("did not find species key %s" % aa["code"])
                aa["species"] = []

        else:
            aa["species"] = []
    if species and "all" in species:
        print("all field was used")
        data.append({"code": 999, "species": species["all"]})

    return data


def make_project_db_json(project_db):

    projects = []

    for i, row in project_db.iterrows():
        print(i)
        p = {
            "id": row[("project", "opr_id", "")],
            "opr_id": row[("project", "opr_id", "")],
            "arb_id": row[("project", "arb_id", "")],
            "name": row[("project", "name", "")],
            "apd": row[("project", "apd", "")],
            "opo": row[("project", "opo", "")],
            "owners": row[("project", "owners", "")],
            "developers": row[("project", "developers", "")],
            "attestor": row[("rp_1", "attestation", "name")],
            "is_opo": row[("rp_1", "attestation", "is_opo")],
            "shape_centroid": row[("project", "shape_centroid", "")],
            "supersection_ids": row[("project", "supersection_ids", "")],
            "acreage": row[("project", "acreage", "")],
            "buffer_contribution": row[("rp_1", "buffer_contribution", "")],
            "arbocs": {
                "issuance": row[("rp_1", "allocation", "issuance")],
                "calculated": row[("rp_1", "allocation", "calculated")],
                "reported": row[("rp_1", "allocation", "reported")],
            },
            "carbon": {
                "initial_carbon_stock": {
                    "value": row[("baseline", "initial_carbon_stock", "")],
                    "units": "tCO2e acre-1",
                },
                "common_practice": {
                    "value": row[("baseline", "common_practice", "")],
                    "units": "tCO2e acre-1",
                },
                "average_slag_baseline": {
                    "value": row[("baseline", "average_slag_baseline", "")],
                    "units": "tCO2e acre-1",
                },
            },
            "baseline": row["baseline"]["components"][
                ["ifm_1", "ifm_3", "ifm_7", "ifm_8"]
            ].to_dict(),
            "rp_1": make_rp_1(row),
            "assessment_areas": make_supersections(row),
            "notes": row["project", "notes", ""],  # include or not?
            "comment": "",  # get from google sheet
        }

        if not p["shape_centroid"]:
            p["shape_centroid"] = []
        else:
            if p["shape_centroid"][0] == -9999:
                p["shape_centroid"] = []

        projects.append(p)

    return projects


def write_project_db_json(project_collection, output):
    with open(output, "w") as outfile:
        json.dump(project_collection, outfile, indent=2)


def write_project_db_csv(db, output):
    df = pd.DataFrame(db)

    dict_cols = ["arbocs", "carbon"]

    for col_key in dict_cols:
        for field_key in df.iloc[0][col_key].keys():
            new_key = f"{col_key}_{field_key}"
            vals = [row[field_key] for row in df[col_key]]
            if isinstance(vals[0], dict):
                vals = [v["value"] for v in vals]
            df[new_key] = vals
    df = df.drop(columns=dict_cols)

    df.to_csv(output)


@click.command()
@click.option('--project-dir')
@click.option('--target-dir')
@click.option('--exclude-graduated-projects/--no-exclude-graduated-projects', default=True)
def main(project_dir, target_dir, exclude_graduated_projects=True):
    fs_prefix, fs_kwargs = get_retro_bucket()

    # Load retro-db and issuance table
    project_db = load_project_db("Forest-Offset-Projects-v0.3", use_cache=False, save=False)

    # There is this strange sub-class of projects within the compliance market which we refer to as
    # "graduated" projects -- these are projects that started out in the "Early Action" period and later
    # "graduated" into the full-fledged compliance program. Unfortunately, these projects tend to be
    # materially deficient and tough to work with -- there are all sorts of issues with getting their
    # numbers correct.
    if exclude_graduated_projects:
        project_db = project_db[~project_db["project"]["early_action"].str.startswith("CAR")]

    ## Get Issuance
    # TODO: store this issuance file somewhere else!
    issuance_table = load_issuance_table(
        f"{project_dir}/documents-of-interest/arb/issuance/arboc_issuance_2020-09-09.xlsx"
    )

    # One project has multiple issuance events in its first reporting period, aggregate them
    agg_by_rp = issuance_table.groupby(["opr_id", "arb_rp_id"])[["allocation"]].sum()
    issuance_first_rp = agg_by_rp.xs("A", level=1)["allocation"]

    ## OPDR-calculated
    # OPDRs report five individual components that we use to recalculate ARBOC issuance:
    # - IFM-1: standing live
    # - IFM-3: standing dead
    # - IFM-7: in-use wood products
    # - IFM-8: landfilled wood products
    # - Secondary Effects: market leakage &etc.

    # We use these five "components", as reported in the OPDR for both the Baseline scenario
    # (imaginary/counterfactual) and the Project scenario (what actually happened), to re-derive the ARBOC
    # allocation. This step (i) gives us confidence in the integrity of our data entry and (ii) lays the
    # foundation for _re-calculating_ ARBOCs under different common practice scenarios (see Notebook TK).
    opdr_calculated = allocation.calculate_allocation(project_db, round_intermediates=False)

    # OPDR-Reported
    reported = project_db[("rp_1", "allocation", "")]

    # Now that we have three Series and can populate the `rp_1.allocation` fields:
    project_db[("rp_1", "allocation", "reported")] = reported
    project_db[("rp_1", "allocation", "calculated")] = opdr_calculated
    project_db[("rp_1", "allocation", "issuance")] = issuance_first_rp

    # TODO -- move?
    write_opdrs(project_db.index, project_dir, target_dir)
    write_ancillary_files(project_dir, target_dir)
    ## Project geometries
    # first copy to target_dir
    write_shapes(project_db.index, project_dir, target_dir)

    # Here we extract the centroid of each project from the project geometries.
    coords = []
    print('getting project centroids')
    for i, proj in tqdm(enumerate(project_db.index)):
        gdf = geopandas.GeoDataFrame.from_file(f"{target_dir}/projects/{proj}/shape.json")
        coords.append(get_centroids(gdf))
    # add project centroids from shapefiles to a new column
    project_db[("project", "shape_centroid", "")] = coords

    project_db_json = make_project_db_json(project_db)

    # write project db
    write_project_db_json(
        project_db_json, f"{target_dir}/forest-offsets-database-{DB_VERSION}.json"
    )
    write_project_db_csv(project_db_json, f"{target_dir}/forest-offsets-database-{DB_VERSION}.csv")

    # copy doc files
    copy_doc_files(target_dir)


if __name__ == '__main__':
    main()
