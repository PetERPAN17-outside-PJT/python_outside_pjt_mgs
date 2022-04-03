from processes.target_data import target_data

def test_get_t_matching_data():
    expect = [{'id': 2}]
    assert expect == target_data().get_t_matching_data()