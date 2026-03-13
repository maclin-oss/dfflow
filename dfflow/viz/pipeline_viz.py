"""
Pipeline visualization utilities (text mode).

Renders sequential and conditional branch nodes in ASCII format.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..core.step import Step
    from ..core.branch import Branch


def text_visualize(nodes: list["Step | Branch"]) -> str:
    """
    Generate ASCII visualization of pipeline structure.

    Parameters
    ----------
    nodes : list[Step | Branch]
        Ordered pipeline nodes.

    Returns
    -------
    str
        Multiline text diagram.
    """

    if not nodes:
        return "Pipeline: (empty)"

    lines = ["Pipeline Flow:", "", "Input"]

    for node in nodes:

        from ..core.branch import Branch

        if isinstance(node, Branch):
            lines.append("  ↓")
            lines.append(f"  ┌─ Branch: {node.name}")

            if node.if_true:
                lines.append("  │  [TRUE]")
                for step in node.if_true:
                    lines.append(f"  │    → {step.name}")
            else:
                lines.append("  │  [TRUE]  → (pass-through)")

            if node.if_false:
                lines.append("  │  [FALSE]")
                for step in node.if_false:
                    lines.append(f"  │    → {step.name}")
            else:
                lines.append("  │  [FALSE] → (pass-through)")

            lines.append("  └─ merge")

        else:
            lines.append("  ↓")
            lines.append(f"  {node.name}")

    lines.append("  ↓")
    lines.append("Output")

    return "\n".join(lines)