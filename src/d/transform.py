import pandas as pd

def set_column_names(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """Assign explicit column names to the DataFrame."""
    df.columns = column_names
    return df

def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are completely empty."""
    return df.dropna(how='all')

def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing values with a default (example: 0)."""
    return df.fillna(0)

def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert columns to appropriate types (example: numbers)."""
    df["class_no"] = df["class_no"].astype(str)
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def apply_regex_to_col_1(df: pd.DataFrame) -> pd.DataFrame:
    """Apply regex to class_no column."""
    df['class_no'] = df['class_no'].str.extract(r'(Total)|(Reinsurance)|(\d+)').stack().droplevel(1)
    return df


def apply_all_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all shared transformation steps in order."""
    df = drop_empty_rows(df)
    df = fill_missing_values(df)
    df = convert_types(df)
    df = apply_regex_to_col_1(df)
    return df

def transform_gwp(df: pd.DataFrame) -> pd.DataFrame:
    df = apply_all_transformations(df)
    # GWP-specific steps
    df['value'] = df['B'] + df['C'] + df['D'] + df['E']
    df = df.drop(columns=['B', 'C', 'D', 'E'])
    df["kpi"] = "gross written premium"
    return df

def transform_claims(df: pd.DataFrame) -> pd.DataFrame:
    df = apply_all_transformations(df)
    # Claims-specific steps
    df['value'] = df['F'] + df['G'] + df['H'] + df['I']
    df = df.drop(columns=['F', 'G', 'H', 'I'])
    df["kpi"] = "gross claims paid"
    return df