import os
import glob
import pandas as pd
import re
from src.d.transform import set_column_names, transform_gwp, transform_claims

def extract_reporting_date(filename: str) -> str:
    match = re.search(r'([1-4])Q(\d{4})', filename)
    if match:
        quarter, year = match.groups()
        return f"Q{quarter}-{year}"
    else:
        return "Unknown"

def process_all_files(raw_dir='data/D/raw', interim_dir='data/D/interim', sheet_name='Tabl.D.2c'):
    os.makedirs(interim_dir, exist_ok=True)
    file_patterns = [os.path.join(raw_dir, '*.xlsx'), os.path.join(raw_dir, '*.xls')]
    files = []
    for pattern in file_patterns:
        files.extend(glob.glob(pattern))
    column_names = ['class_no', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
    skiprows = 7
    for file_path in files:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skiprows)
            df = set_column_names(df, column_names)
            df_gwp: pd.DataFrame = pd.DataFrame(df[["class_no", "B", "C", "D", "E"]])
            df_claims: pd.DataFrame = pd.DataFrame(df[["class_no", "F", "G", "H", "I"]])
            df_gwp = transform_gwp(df_gwp)
            df_claims = transform_claims(df_claims)
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            reporting_period = extract_reporting_date(base_name)
            df_gwp["reporting_period"] = reporting_period
            df_claims["reporting_period"] = reporting_period

            # Add reporting_quarter as integer 1-4 derived from reporting_period
            def extract_quarter(date_str):
                match = re.match(r"Q([1-4])-", date_str)
                return int(match.group(1)) if match else None
            df_gwp["reporting_quarter"] = df_gwp["reporting_period"].apply(extract_quarter)
            df_claims["reporting_quarter"] = df_claims["reporting_period"].apply(extract_quarter)

            # Add reporting_year as integer 2022-2099 derived from reporting_period
            def extract_year(date_str):
                match = re.match(r"Q[1-4]-(20[2-9][0-9])", date_str)
                return int(match.group(1)) if match else None
            df_gwp["reporting_year"] = df_gwp["reporting_period"].apply(extract_year)
            df_claims["reporting_year"] = df_claims["reporting_period"].apply(extract_year)

            # Add reporting_date as German format DD.MM.YYYY for the last day of the reporting period 
            def get_last_day_of_quarter_de(row):
                quarter = row["reporting_quarter"]
                year = row["reporting_year"]
                if quarter == 1:
                    return f"31.03.{year}"
                elif quarter == 2:
                    return f"30.06.{year}"
                elif quarter == 3:
                    return f"30.09.{year}"
                elif quarter == 4:
                    return f"31.12.{year}"
                else:
                    return None
            df_gwp["reporting_date"] = df_gwp.apply(get_last_day_of_quarter_de, axis=1)
            df_claims["reporting_date"] = df_claims.apply(get_last_day_of_quarter_de, axis=1)
            # Convert reporting_date to datetime type
            df_gwp["reporting_date"] = pd.to_datetime(df_gwp["reporting_date"], format="%d.%m.%Y")
            df_claims["reporting_date"] = pd.to_datetime(df_claims["reporting_date"], format="%d.%m.%Y")

            output_gwp = os.path.join(interim_dir, f'{base_name}_gwp.csv')
            output_claims = os.path.join(interim_dir, f'{base_name}_claims.csv')
            df_gwp.to_csv(output_gwp, index=False)
            df_claims.to_csv(output_claims, index=False)
            print(f"Processed and saved: {output_gwp} and {output_claims}")
        except Exception as e:
            print(f"Failed to process {file_path}: {e}")

if __name__ == "__main__":
    process_all_files() 