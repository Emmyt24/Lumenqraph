#!/usr/bin/env python3
"""Validate that Grafana dashboard metrics match those defined in Rust source code.

This script ensures dashboard panels reference only metrics that actually exist
in the codebase, catching drift early.
"""
import json
import re
import sys
from pathlib import Path


def extract_metrics_from_dashboard(dashboard_path: str) -> set[str]:
    """Extract all metric names referenced in Grafana dashboard."""
    with open(dashboard_path) as f:
        dashboard = json.load(f)

    metrics = set()
    for panel in dashboard.get("panels", []):
        for target in panel.get("targets", []):
            expr = target.get("expr", "")
            if expr:
                # Extract metric names from Prometheus expressions
                # Patterns: metric_name, rate(metric_name), histogram_quantile(...metric_name...)
                found = re.findall(r"\b(lumenqraph_\w+)\b", expr)
                metrics.update(found)

    return metrics


def extract_metrics_from_rust(rust_dir: str) -> set[str]:
    """Extract all metric names defined in Rust source code."""
    metrics = set()

    # Scan every Rust source file so counters defined outside `metrics.rs`
    # (e.g. `store.rs`, `retention.rs`, `http.rs`) are also discovered.
    for rust_file in Path(rust_dir).rglob("*.rs"):
        with open(rust_file) as f:
            content = f.read()
            # Find all metric definitions: "lumenqraph_metric_name"
            found = re.findall(r'"(lumenqraph_\w+)"', content)
            metrics.update(found)

    return metrics


def main() -> int:
    repo_root = Path(__file__).parent.parent
    dashboard_path = repo_root / "monitoring" / "grafana_dashboard.json"
    rust_crates = repo_root / "crates"

    if not dashboard_path.exists():
        print(f"error: dashboard not found at {dashboard_path}", file=sys.stderr)
        return 1

    try:
        dashboard_metrics = extract_metrics_from_dashboard(str(dashboard_path))
        rust_metrics = extract_metrics_from_rust(str(rust_crates))
    except Exception as e:
        print(f"error reading files: {e}", file=sys.stderr)
        return 1

    # Find metrics in dashboard but not in code
    missing = dashboard_metrics - rust_metrics
    if missing:
        print("error: dashboard references metrics not found in code:", file=sys.stderr)
        for metric in sorted(missing):
            print(f"  - {metric}", file=sys.stderr)
        return 1

    # Find metrics in code but not in dashboard (warning only)
    unused = rust_metrics - dashboard_metrics
    if unused:
        print("warning: metrics in code but not in dashboard:", file=sys.stderr)
        for metric in sorted(unused):
            print(f"  - {metric}", file=sys.stderr)

    print(f"✓ dashboard references {len(dashboard_metrics)} valid metrics", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
