import numpy as np
import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the input DataFrame by handling missing values."""
    missing_before = df.isna().sum()
    print(f'Missing values before cleaning:\n{missing_before}')
    df = df.replace({np.nan: None, 'X': None, pd.NA: None})
    df = df.dropna()
    print(f'Number of rows after cleaning: {len(df):,}')
    return df
