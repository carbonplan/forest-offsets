import json
from pathlib import Path

import fsspec
import geopandas


def get_opr_ids() -> list:
    """Return list of all opr ids

    Returns:
        list -- all OPR ids as string
    """
    fs = fsspec.filesystem("gcs")
    fnames = fs.ls("carbonplan-scratch/carb-geometries/topo")
    opr_ids = [Path(fname).stem for fname in fnames]
    return opr_ids


def load_project_geometry(opr_id: str) -> geopandas.GeoDataFrame:
    with fsspec.open(
        f"https://storage.googleapis.com/carbonplan-forest-offsets/carb-geometries/raw/{opr_id}.json"
    ) as f:
        d = json.load(f)

    geo = geopandas.GeoDataFrame.from_features(d)
    geo = geo.set_crs("epsg:4326")
    geo.geometry = geo.buffer(0)
    return geo
