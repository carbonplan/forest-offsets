from carbonplan_forest_offsets.load.issuance import get_arb_id_map, load_issuance_table


def test_load_issuance_table():
    all_projects = load_issuance_table(forest_only=False)
    forest_projects = load_issuance_table(forest_only=True)
    assert len(all_projects) > len(forest_projects)


def test_get_arb_id_map():
    arb_id_map = get_arb_id_map()
    assert isinstance(arb_id_map, dict)
