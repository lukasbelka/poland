# This script will combine all interim CSVs into a single DataFrame and save to processed.


import os
import glob
import pandas as pd

def combine_csvs(interim_dir, pattern, output_path):
    files = glob.glob(os.path.join(interim_dir, pattern))
    if not files:
        print(f"No files found for pattern {pattern} in {interim_dir}")
        return
    df_list = [pd.read_csv(f) for f in files]
    combined = pd.concat(df_list, ignore_index=True)
    combined.to_csv(output_path, index=False)
    print(f"Combined {len(files)} files into {output_path}")

def main():
    interim_dir = "data/D/interim"
    interim_combined_dir = "data/D/interim_combined"
    os.makedirs(interim_combined_dir, exist_ok=True)

    # Combine GWP files
    combine_csvs(interim_dir, "*_gwp.csv", os.path.join(interim_combined_dir, "combined_gwp.csv"))

    # Combine claims files
    combine_csvs(interim_dir, "*_claims.csv", os.path.join(interim_combined_dir, "combined_claims.csv"))

if __name__ == "__main__":
    main() 