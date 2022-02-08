import geopandas
import prefect
import requests
import topojson

CARB_GIS_ENDPOINT = (
    "https://gis.carb.arb.ca.gov/hosting/rest/services/ARBOC_Issuance_Map/MapServer/0/query"
)


# TODO: Merge 'get most recent' into load
# TODO: from issuance get arb_id_opr_id map
# once we have that, template outputs

@prefect.task
def get_object_ids() -> list:

    params = {"where": "OBJECTID > 0", "returnIdsOnly": 'true', "f": "pjson"}
    r = requests.get(CARB_GIS_ENDPOINT, params=params)
    return r.json()['objectIds']


@prefect.task
def get_project_geometry(object_id: int) -> geopandas.GeoDataFrame:

    params = {
        "objectIds": f"{object_id}",
        "outFields": "arb_id",
        "geometryPrecision": "5",
        "f": "geojson",
    }
    r = requests.get(CARB_GIS_ENDPOINT, params=params)
    gdf = geopandas.GeoDataFrame.from_features(r.json())
    return gdf

def generate_task_run_name(blah):
    pass

@prefect.task(result=LocalResult(), target="")
def get_simplified_project_geometry(project_geometry: geopandas.GeoDataFrame) -> str:
    topo = topojson.Topology(project_geometry)
    simplified = topo.toposimplify(0.001, prevent_oversimplify=True)
    return simplified.to_json()


with prefect.Flow(name="carb-geometries") as flow:
    object_ids = get_object_ids()
    geometries = get_project_geometry.map(object_ids)
    simplified = get_simplified_project_geometry.map(geometries)
