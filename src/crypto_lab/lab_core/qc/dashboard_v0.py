from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List

import pandas as pd


def _preview_rows(df: pd.DataFrame, indices: List[int], context: int = 2) -> str:
    """Сформировать preview строк вокруг проблемных индексов."""
    previews = []
    n = len(df)

    for idx in indices:
        start = max(0, idx - context)
        end = min(n, idx + context + 1)
        snippet = df.iloc[start:end].to_markdown(index=True)
        previews.append(f"### Around index {idx}\n\n" + snippet)

    return "\n\n".join(previews)


def render_qc_markdown(qc: Dict[str, Any], df: pd.DataFrame) -> str:
    """Сгенерировать полноценный QC-отчёт в Markdown."""

    lines = []

    lines.append("# QC Report (v0)\n")

    # --------------------- Summary ---------------------
    lines.append("## Summary\n")
    for key in [
        "structure_ok",
        "dtype_ok",
        "duplicates",
        "missing_timestamps",
        "dirty_price_rows",
        "negative_values",
    ]:
        value = qc.get(key)
        lines.append(f"- **{key}**: {value}")

    lines.append("\n---\n")

    # --------------------- Missing timestamps ---------------------
    missing = qc.get("missing_timestamps", [])
    lines.append("## Missing Timestamps\n")
    if missing:
        for ts in missing:
            lines.append(f"- {ts}")
    else:
        lines.append("No missing timestamps found.")
    lines.append("\n---\n")

    # --------------------- Dirty price rows ---------------------
    dirty = qc.get("dirty_price_rows", [])
    lines.append("## Dirty Price Rows\n")
    if dirty:
        lines.append("Indices:\n")
        for idx in dirty:
            lines.append(f"- {idx}")
        lines.append("\n### Previews\n")
        lines.append(_preview_rows(df, dirty))
    else:
        lines.append("No dirty price rows found.")
    lines.append("\n---\n")

    # --------------------- Negative values ---------------------
    negative = qc.get("negative_values", [])
    lines.append("## Negative Values\n")
    if negative:
        lines.append("Indices:\n")
        for idx in negative:
            lines.append(f"- {idx}")
        lines.append("\n### Previews\n")
        lines.append(_preview_rows(df, negative))
    else:
        lines.append("No negative values found.")
    lines.append("\n---\n")

    # --------------------- Dataset overview ---------------------
    lines.append("## Dataset Overview\n")
    lines.append(f"- Shape: {df.shape}")
    if not df.empty:
        lines.append(f"- Timestamp range: {df['timestamp'].min()} .. {df['timestamp'].max()}")
        lines.append("\n### Head\n")
        lines.append(df.head().to_markdown(index=True))
    else:
        lines.append("Dataset is empty.")

    lines.append("\n")

    return "\n".join(lines)


def save_qc_markdown(md_str: str, output_path: Path | str) -> Path:
    """Сохранить markdown-отчёт."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(md_str, encoding="utf-8")
    return output_path


def pipeline_qc_dashboard(
    df: pd.DataFrame,
    qc: Dict[str, Any],
    symbol: str,
    timeframe: str,
    output_dir: Path | str = "data/qc_reports"
) -> Path:
    """Сформировать и сохранить QC-отчёт в Markdown."""

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    symbol_safe = symbol.replace("/", "-")

    output_dir = Path(output_dir) / symbol_safe / timeframe
    filename = f"qc_report_{now_ms}.md"
    full_path = output_dir / filename

    md = render_qc_markdown(qc, df)
    save_qc_markdown(md, full_path)

    return full_path


__all__ = [
    "render_qc_markdown",
    "save_qc_markdown",
    "pipeline_qc_dashboard",
]
