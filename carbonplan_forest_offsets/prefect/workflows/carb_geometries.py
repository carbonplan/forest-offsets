import datetime
import json

import geopandas
import prefect
import requests
import topojson
from prefect.engine.results import GCSResult

from carbonplan_forest_offsets.load.issuance import load_issuance_table

CARB_GIS_ENDPOINT = (
    "https://gis.carb.arb.ca.gov/hosting/rest/services/ARBOC_Issuance_Map/MapServer/0/query"
)


serializer = prefect.engine.serializers.JSONSerializer()
result = GCSResult(bucket="carbonplan-forest-offsets", serializer=serializer)


@prefect.task
def get_object_ids() -> list:
    """Hit MapServer endpoint to get list of stored object ids.

    Returns:
        list -- ObjectIDs that used to query individual project geometries
    """
    params = {"where": "OBJECTID > 0", "returnIdsOnly": "true", "f": "pjson"}
    r = requests.get(CARB_GIS_ENDPOINT, params=params)
    return r.json()["objectIds"]


@prefect.task(cache_for=datetime.timedelta(hours=1))
def get_arb_id_to_opr_id_map() -> dict:
    """Translate from ARB internal ID (i.e., CAFR0001) to OPR IDs (i.e., ACR101)

    Returns:
        dict -- key-value store of arb_id to opr_id
    """
    df = load_issuance_table(forest_only=False, most_recent=True)[["opr_id", "arb_id"]].set_index(
        "arb_id"
    )
    return df.opr_id.to_dict()


@prefect.task
def get_project_geometry(object_id: int, name_map: dict) -> geopandas.GeoDataFrame:
    """Download project geometry and append metadata

    Arguments:
        object_id {int} -- Unique id for project geometry
        name_map {dict} -- map from ARB ID to OPR ID

    Returns:
        geopandas.GeoDataFrame -- [description]
    """
    params = {
        "objectIds": f"{object_id}",
        "outFields": "arb_id",
        "geometryPrecision": "5",
        "f": "geojson",
    }
    r = requests.get(CARB_GIS_ENDPOINT, params=params)
    gdf = geopandas.GeoDataFrame.from_features(r.json())

    arb_id = gdf.get("arb_id").item()
    gdf["opr_id"] = name_map.get(arb_id)
    return gdf


def generate_raw_target_name(project_geometry: geopandas.GeoDataFrame, **kwargs) -> str:
    """Filename template for raw data.

    Arguments:
        project_geometry {geopandas.GeoDataFrame} -- project geom being written.
        N.B., prefect expects templating funcitons to take same inputs as Task, so we have to pass GeoDataFrame as opposed to opr_id.


    Returns:
        str -- pathname we'll write to
    """
    opr_id = project_geometry.get("opr_id").item()
    return f"carb-geometries/raw/{opr_id}.json"


def generate_topo_target_name(project_geometry: geopandas.GeoDataFrame, **kwargs) -> str:
    """Filename template for topo data.

    Arguments:
        project_geometry {geopandas.GeoDataFrame} -- project geom being written.
        N.B., prefect expects templating funcitons to take same inputs as Task, so we have to pass GeoDataFrame as opposed to opr_id.

    Returns:
        str -- pathname we'll write to
    """
    opr_id = project_geometry.get("opr_id").item()
    return f"carb-geometries/topo/{opr_id}.json"


@prefect.task(
    target=generate_raw_target_name,
    result=result,
    checkpoint=True,
)
def cache_project_geometry(project_geometry: geopandas.GeoDataFrame) -> str:
    return json.loads(
        project_geometry.to_json()
    )  # to_dict errors so just convert to_json string and back


@prefect.task(
    target=generate_topo_target_name,
    result=result,
    checkpoint=True,
)
def get_simplified_project_geometry(project_geometry: geopandas.GeoDataFrame) -> str:
    topo = topojson.Topology(project_geometry)
    simplified = topo.toposimplify(0.001, prevent_oversimplify=True)
    return json.loads(
        simplified.to_json()
    )  # to_dict errors so just convert to_json string and back


with prefect.Flow(name="download-carb-geometries") as flow:
    name_map = get_arb_id_to_opr_id_map()
    object_ids = get_object_ids()
    geometries = get_project_geometry.map(object_id=object_ids, name_map=prefect.unmapped(name_map))
    cache_project_geometry.map(geometries)
    simplified = get_simplified_project_geometry.map(geometries)
