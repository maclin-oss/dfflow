import pytest
import pandas as pd

from dfflow.core.branch import Branch
from dfflow.core.step import Step


@pytest.fixture
def sample_df():
    return pd.DataFrame({"revenue": [100.0, 200.0, 300.0]})


@pytest.fixture
def true_step():
    return Step("True Step", lambda df: df.assign(path="true"))


@pytest.fixture
def false_step():
    return Step("False Step", lambda df: df.assign(path="false"))


def test_branch_valid_construction(true_step, false_step):
    branch = Branch(
        name="Test Branch",
        condition=lambda df: True,
        if_true=[true_step],
        if_false=[false_step],
    )
    assert branch.name == "Test Branch"
    assert len(branch.if_true) == 1
    assert len(branch.if_false) == 1


def test_branch_if_false_empty(true_step):
    branch = Branch(
        name="One Path",
        condition=lambda df: True,
        if_true=[true_step],
        if_false=[],
    )
    assert branch.if_false == []


def test_branch_empty_name_raises(true_step):
    with pytest.raises(ValueError):
        Branch(name="", condition=lambda df: True, if_true=[true_step])


def test_branch_non_callable_condition_raises(true_step):
    with pytest.raises(TypeError):
        Branch(name="Bad", condition="not_callable", if_true=[true_step])


def test_branch_both_paths_empty_raises():
    with pytest.raises(ValueError):
        Branch(name="Empty", condition=lambda df: True, if_true=[], if_false=[])


def test_branch_invalid_step_type_raises():
    with pytest.raises(TypeError):
        Branch(name="Bad", condition=lambda df: True, if_true=["not_a_step"])


def test_branch_repr(true_step, false_step):
    branch = Branch(
        name="Router",
        condition=lambda df: True,
        if_true=[true_step],
        if_false=[false_step],
    )
    r = repr(branch)
    assert "Router" in r
    assert "if_true=1" in r
    assert "if_false=1" in r