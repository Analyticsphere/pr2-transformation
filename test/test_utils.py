import pytest

from core.utils import extract_ordered_concept_ids, extract_loop_number, group_vars_by_cid_and_loop_num


# Test cases for extract_cids_from_var_name
@pytest.mark.parametrize("var_name, expected", [
    ("d_123456789_d_987654321", ["123456789", "987654321"]),
    ("D_123456789_987654321", ["123456789"]),
    ("D_123412349_1_1_D_987654321_1_1", ["123412349", "987654321"]),
    ("d_999999999", ["999999999"]),
    ("D_812370563_1_1_D_812370563_1_1_D_665036297", ["812370563", "812370563", "665036297"]),
    ("D_812370563_1_1_D_812370563_V3_1_1_D_665036297", ["812370563", "812370563", "665036297"]),
    ("random_text", []),  # No concept ID should return an empty list
])
def test_extract_ordered_concept_ids(var_name, expected):
    assert extract_ordered_concept_ids(var_name) == expected


# Test cases for extract_loop_number
@pytest.mark.parametrize("var_name, expected", [
    ("d_123456789_1_1_d_987654321_1_1", 1),
    ("d_123456789_2_2_d_987654321_2_2", 2),
    ("d_111111111_1_1_d_222222222_1_1", 1),
    ("d_123456789_9_9_d_987654321_9_9", 9),
    ("d_123456789_9_9_d_987654321_9_9_9_9_9_9", 9),
    ("d_123456789_9_9_d_987654321_v1_9_9_9_9_9_9", 9),
    ("d_123456789_v3_9_9_d_987654321_9_9_9_9_9_9", 9),
    ("d_123456789_5_5", 5),
    ("d_123456789", None),  # No loop number, should return None
    ("d_111111111_12_12_d_222222222_12_12", 12),
])

def test_extract_loop_number(var_name, expected):
    assert extract_loop_number(var_name) == expected


# Test cases for group_by_concept_and_loop
def test_group_vars_by_cid_and_loop_num():
    var_list = [
        "d_123456789_1_1_d_987654321_1_1",
        "d_123456789_2_2_d_987654321_2_2",
        "d_111111111_1_1_d_222222222_1_1_v1",
        "d_123456789_9_9_d_987654321_9_9",
        "d_123456789_9_9_d_987654321_9_9_9_9_9_9",
        "d_123456789_v3_5_5",
        "d_123456789"  # No loop number, should be ignored
    ]

    expected_output = {
        (frozenset({"123456789", "987654321"}), 1, ""): ["d_123456789_1_1_d_987654321_1_1"],
        (frozenset({"123456789", "987654321"}), 2, ""): ["d_123456789_2_2_d_987654321_2_2"],
        (frozenset({"111111111", "222222222"}), 1, "_v1"): ["d_111111111_1_1_d_222222222_1_1_v1"],
        (frozenset({"123456789", "987654321"}), 9, ""): [
            "d_123456789_9_9_d_987654321_9_9",
            "d_123456789_9_9_d_987654321_9_9_9_9_9_9"
        ],
        (frozenset({"123456789"}), 5, "_v3"): ["d_123456789_v3_5_5"]
    }

    result = group_vars_by_cid_and_loop_num(var_list)
    assert result == expected_output
