"""
This script will load the combined data from data/D/interim_combined/ and perform ISO-specific calculations.
"""
import pandas as pd
import os

def calculate_isolated_quarters(input_path, output_path):
    df = pd.read_csv(input_path, parse_dates=["reporting_date"])
    # Sort by group, year, and quarter
    df = df.sort_values(["class_no", "reporting_year", "reporting_quarter"])
    # Calculate isolated values from YTD 'iso' column
    df["iso"] = df.groupby(["class_no", "reporting_year"])["ytd"].diff().fillna(df["ytd"])
    # Calculate absolute change from previous iso value, but set to 0 for Q1
    df["iso_chng_pq_abs"] = df["iso"].diff()
    df.loc[df["reporting_quarter"] == 1, "iso_chng_pq_abs"] = float(0)
    # Calculate percentage change from previous iso value, but set to 0 for Q1
    df["iso_chng_pq_pct"] = df["iso"].pct_change().fillna(0)
    df.loc[df["reporting_quarter"] == 1, "iso_chng_pq_pct"] = float(0)
    df.to_csv(output_path, index=False)
    print(f"Saved isolated quarterly values to {output_path}")

def main():
    # Claims
    input_claims = "data/D/interim_combined/combined_claims.csv"
    output_claims = "data/D/interim_combined/combined_claims_isolated.csv"
    calculate_isolated_quarters(input_claims, output_claims)

    # GWP
    input_gwp = "data/D/interim_combined/combined_gwp.csv"
    output_gwp = "data/D/interim_combined/combined_gwp_isolated.csv"
    calculate_isolated_quarters(input_gwp, output_gwp)

if __name__ == "__main__":
    main()
