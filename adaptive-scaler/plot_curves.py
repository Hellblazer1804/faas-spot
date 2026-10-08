import argparse
import json
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
os.environ.setdefault("MPLCONFIGDIR", str(SCRIPT_DIR / ".matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(SCRIPT_DIR / ".cache"))
Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
Path(os.environ["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _normalize_time(xs: np.ndarray, base_unit: str):
    if xs.size == 0:
        return xs, base_unit
    max_val = float(np.max(xs))
    if base_unit == "seconds":
        xs = xs / 60.0
        base_unit = "minutes"
        max_val = max_val / 60.0
    if base_unit == "minutes" and max_val >= 180.0:
        xs = xs / 60.0
        base_unit = "hours"
    return xs, base_unit


def _load_trace_or_cdf(csv_path: Path):
    df = pd.read_csv(csv_path)
    lifetime_col = None
    for col in ["Lifetime", "Lifetime_seconds", "Lifetime_minutes"]:
        if col in df.columns:
            lifetime_col = col
            break
    if lifetime_col is None:
        raise ValueError(f"{csv_path} missing lifetime column")

    lifetimes = df[lifetime_col].astype(float).to_numpy()
    if lifetime_col == "Lifetime_seconds":
        base_unit = "seconds"
    elif lifetime_col == "Lifetime_minutes":
        base_unit = "minutes"
    else:
        base_unit = "seconds" if np.nanmax(lifetimes) > 1000 else "minutes"
    if "CDF" in df.columns:
        cdf_df = df[[lifetime_col, "CDF"]].astype(float)
        cdf_df = cdf_df.sort_values([lifetime_col, "CDF"])
        cdf_df = cdf_df.groupby(lifetime_col, as_index=False)["CDF"].max()
        xs = cdf_df[lifetime_col].to_numpy()
        xs, unit = _normalize_time(xs, base_unit)
        cdf = cdf_df["CDF"].to_numpy()
        survival = 1.0 - cdf
        return xs, cdf, survival, unit

    # Kaplan-Meier survival curve for lifetime traces
    lifetimes = np.sort(lifetimes)
    n = len(lifetimes)
    if n == 0:
        return np.array([]), np.array([]), np.array([]), base_unit
    unique_times, counts = np.unique(lifetimes, return_counts=True)
    at_risk = n
    survival_vals = []
    cdf_vals = []
    surv = 1.0
    cum = 0
    for t, d in zip(unique_times, counts):
        if at_risk > 0:
            surv *= (at_risk - d) / at_risk
        else:
            surv = 0.0
        survival_vals.append(surv)
        cum += d
        cdf_vals.append(cum / n)
        at_risk -= d
    unique_times, unit = _normalize_time(unique_times, base_unit)
    return unique_times, np.array(cdf_vals), np.array(survival_vals), unit


def _load_survival_json(json_path: Path):
    with json_path.open() as f:
        survival = json.load(f)
    xs = np.array([float(k) for k in survival.keys()])
    ys = np.array([float(v) for v in survival.values()])
    order = np.argsort(xs)
    xs = xs[order]
    ys = ys[order]
    cdf = 1.0 - ys
    xs, unit = _normalize_time(xs, "minutes")
    return xs, cdf, ys, unit


def _plot_step(xs, ys, title, x_label, y_label, out_base: Path, formats):
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.step(xs, ys, where="post")
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_ylim(0, 1.0)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    for fmt in formats:
        fig.savefig(out_base.with_suffix(f".{fmt}"), dpi=150)
    plt.close(fig)


def plot_from_spot_traces(spot_traces_dir: Path, out_dir: Path, formats):
    csv_files = sorted(spot_traces_dir.glob("*.csv"))
    for csv_path in csv_files:
        xs, cdf, survival, unit = _load_trace_or_cdf(csv_path)
        base = csv_path.stem
        _plot_step(
            xs,
            cdf,
            title=f"CDF: {base}",
            x_label=f"Lifetime ({unit})",
            y_label="CDF",
            out_base=out_dir / f"{base}_cdf",
            formats=formats,
        )
        _plot_step(
            xs,
            survival,
            title=f"Survival: {base}",
            x_label=f"Lifetime ({unit})",
            y_label="Survival Probability",
            out_base=out_dir / f"{base}_survival",
            formats=formats,
        )


def plot_from_survival_curves(
    survival_dir: Path, out_dir: Path, formats, only_file: Path | None = None
):
    if only_file is not None:
        json_files = [only_file]
    else:
        json_files = sorted(survival_dir.glob("*.json"))
    for json_path in json_files:
        xs, cdf, survival, unit = _load_survival_json(json_path)
        base = json_path.stem
        _plot_step(
            xs,
            cdf,
            title=f"CDF: {base}",
            x_label=f"Lifetime ({unit})",
            y_label="CDF",
            out_base=out_dir / f"{base}_cdf",
            formats=formats,
        )
        _plot_step(
            xs,
            survival,
            title=f"Survival: {base}",
            x_label=f"Lifetime ({unit})",
            y_label="Survival Probability",
            out_base=out_dir / f"{base}_survival",
            formats=formats,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Generate CDF and survival curve plots."
    )
    script_dir = SCRIPT_DIR
    parser.add_argument(
        "--spot-traces-dir",
        default=str(script_dir / "spot-traces-csv"),
        help="Directory with spot trace CSV files.",
    )
    parser.add_argument(
        "--survival-curves-dir",
        default=str(script_dir / "survival_curves"),
        help="Directory with survival curve JSON files.",
    )
    parser.add_argument(
        "--survival-curve-file",
        default="",
        help="Specific survival curve JSON file to plot.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(script_dir / "curve_plots"),
        help="Directory to write plots.",
    )
    parser.add_argument(
        "--formats",
        default="png,svg",
        help="Comma-separated output formats (e.g., png,svg).",
    )
    args = parser.parse_args()

    spot_traces_dir = Path(args.spot_traces_dir)
    survival_dir = Path(args.survival_curves_dir)
    out_dir = Path(args.out_dir)
    _ensure_dir(out_dir)

    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]
    if spot_traces_dir.exists():
        plot_from_spot_traces(spot_traces_dir, out_dir, formats)
    if args.survival_curve_file:
        only_file = Path(args.survival_curve_file)
        if only_file.exists():
            plot_from_survival_curves(
                only_file.parent, out_dir, formats, only_file=only_file
            )
    elif survival_dir.exists():
        plot_from_survival_curves(survival_dir, out_dir, formats)


if __name__ == "__main__":
    main()
