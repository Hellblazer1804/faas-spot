import pandas as pd
import numpy as np
import json
import sys
import os

def build_survival_curve_from_trace(lifetimes):
    lifetimes = np.sort(lifetimes)
    N = len(lifetimes)
    unique_times = np.unique(lifetimes)
    survival = []
    survivors = N
    last_survival = 1.0
    for t in unique_times:
        deaths = np.sum(lifetimes == t)
        prob = (survivors - deaths) / survivors if survivors > 0 else 0
        last_survival = last_survival * prob
        survival.append((int(t), float(last_survival)))
        survivors -= deaths
    return dict(survival)

def build_survival_curve_from_cdf(df):
    # Survival = 1 - CDF
    survival = {int(row["Lifetime"]): float(1.0 - row["CDF"]) for _, row in df.iterrows()}
    return survival

def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_survival_curve.py <input_file>")
        sys.exit(1)

    in_file = sys.argv[1]
    basename = os.path.basename(in_file).replace(".csv", "")
    out_file = f"survival_curves/{basename}_survival_curve.json"

    df = pd.read_csv(in_file)
    # Detect format
    if "Lifetime" in df.columns and "CDF" in df.columns:
        print("Detected CDF input.")
        survival_curve = build_survival_curve_from_cdf(df)
    elif "Lifetime" in df.columns:
        print("Detected lifetime trace input.")
        survival_curve = build_survival_curve_from_trace(df["Lifetime"].values)
    else:
        print("Input must have 'Lifetime' (and optional 'CDF') columns.")
        sys.exit(1)

    with open(out_file, "w") as f:
        json.dump(survival_curve, f, indent=2)
    print(f"✅ Survival curve written to {out_file}")

if __name__ == "__main__":
    main()
