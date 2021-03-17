import pytest

from carbonplan_retro.load.assessment_area import load_assessment_areas
from carbonplan_retro.load.geometry import ifm_shapes
from carbonplan_retro.load.issuance import get_arb_id_map, load_issuance_table


@pytest.mark.parametrize('year', [2011, 2014, 2015])
def test_load_assessment_areas(year):
    df = load_assessment_areas(year)
    assert len(df)


def test_ifm_shapes():
    # TODO: parameterize
    df = ifm_shapes()
    assert len(df)


def test_load_issuance_table():
    all_projects = load_issuance_table(forest_only=False)
    forest_projects = load_issuance_table(forest_only=True)
    assert len(all_projects) > len(forest_projects)


def test_get_arb_id_map():
    arb_id_map = get_arb_id_map()
    assert isinstance(arb_id_map, dict)
