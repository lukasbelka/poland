"""
This script will load the combined data from data/D/interim_combined/ and perform ISO-specific calculations.
"""
import pandas as pd
import numpy as np


def calculate_isolated_and_changes(
    df, 
    ytd_col, 
    iso_col, 
    abs_col, 
    pct_col
):
    df = df.sort_values(["class_no", "reporting_year", "reporting_quarter"])
    # Isolated value
    df[iso_col] = df.groupby(["class_no", "reporting_year"])[ytd_col].diff().fillna(df[ytd_col])
    # Absolute change (NaN for Q1)
    df[abs_col] = df[iso_col].diff()
    df.loc[(df["reporting_quarter"] == 1) & (df["reporting_year"] == 2022), abs_col] = float("nan")
    # Percentage change (0 for Q1)
    df[pct_col] = df[iso_col].pct_change().fillna(0) * 100
    df.loc[(df["reporting_quarter"] == 1) & (df["reporting_year"] == 2022), pct_col] = float(0)
    return df

def process_file(
    input_path, 
    output_path, 
    ytd_col, 
    iso_col, 
    abs_col, 
    pct_col
):
    df = pd.read_csv(input_path, parse_dates=["reporting_date"])
    df = calculate_isolated_and_changes(df, ytd_col, iso_col, abs_col, pct_col)
    df.to_csv(output_path, index=False)
    print(f"Saved with {iso_col}, {abs_col}, and {pct_col} to {output_path}")

def main():
    # Claims (GCP)
    process_file(
        "data/D/interim_combined/combined_claims.csv",
        "data/D/interim_combined/combined_claims_isolated.csv",
        ytd_col="gcp_ytd",
        iso_col="iso_gcp",
        abs_col="iso_gcp_chng_pq_abs",
        pct_col="iso_gcp_chng_pq_pct"
    )
    # GWP
    process_file(
        "data/D/interim_combined/combined_gwp.csv",
        "data/D/interim_combined/combined_gwp_isolated.csv",
        ytd_col="gwp_ytd",  # Change to your actual GWP YTD column name
        iso_col="iso_gwp",
        abs_col="iso_gwp_chng_pq_abs",
        pct_col="iso_gwp_chng_pq_pct"
    )

if __name__ == "__main__":
    main()

