from typing import TYPE_CHECKING

from prefect import Flow
from prefect.core.parameter import DateTimeParameter

from carbonplan_forest_offsets.prefect.tasks import nifc

if TYPE_CHECKING:
    import geopandas


def calculate_burned_area(
    project: geopandas.GeoDataFrame, fire_perimeters: geopandas.GeoDataFrame
) -> float:
    pass


with Flow('project-burned-stats') as flow:
    as_of = DateTimeParameter("as_of")

    nifc_perimeters = nifc.load_nifc_asof(as_of)
