from dfflow.viz.pipeline_viz import text_visualize
from dfflow.core.step import Step
from dfflow.core.branch import Branch


def test_text_visualize_single_step():
    steps = [Step("Step1", lambda df: df)]
    output = text_visualize(steps)

    assert "Pipeline Flow:" in output
    assert "Input" in output
    assert "Step1" in output
    assert "Output" in output


def test_text_visualize_empty():
    output = text_visualize([])
    assert output == "Pipeline: (empty)"


def test_text_visualize_multiple_steps():
    steps = [
        Step("Step1", lambda df: df),
        Step("Step2", lambda df: df),
    ]
    output = text_visualize(steps)

    assert "Step1" in output
    assert "Step2" in output


def test_text_visualize_branch():
    nodes = [
        Branch(
            name="Revenue Router",
            condition=lambda df: True,
            if_true=[Step("Premium", lambda df: df)],
            if_false=[Step("Standard", lambda df: df)],
        )
    ]
    output = text_visualize(nodes)

    assert "Branch: Revenue Router" in output
    assert "[TRUE]" in output
    assert "[FALSE]" in output
    assert "Premium" in output
    assert "Standard" in output
    assert "merge" in output


def test_text_visualize_branch_empty_false_path():
    nodes = [
        Branch(
            name="Null Guard",
            condition=lambda df: True,
            if_true=[Step("Drop Nulls", lambda df: df)],
            if_false=[],
        )
    ]
    output = text_visualize(nodes)

    assert "pass-through" in output


def test_text_visualize_mixed_nodes():
    nodes = [
        Step("Lowercase", lambda df: df),
        Branch(
            name="Router",
            condition=lambda df: True,
            if_true=[Step("Tag Premium", lambda df: df)],
            if_false=[Step("Tag Standard", lambda df: df)],
        ),
        Step("Final Step", lambda df: df),
    ]
    output = text_visualize(nodes)

    assert "Lowercase" in output
    assert "Branch: Router" in output
    assert "Final Step" in output