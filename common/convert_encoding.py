import glob
import os

import pandas as pd


def convert_csv_encoding(path_pattern: str) -> None:
    """
    Convert CSV files from CP949 to UTF-8 encoding.

    Args:
        path_pattern (str): Glob pattern for matching CSV files (e.g., './raw_data/**/*.csv').
    """
    csv_files = glob.glob(path_pattern, recursive=True)
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, encoding='cp949')
            output_path = f"{os.path.splitext(file_path)[0]}.csv"
            df.to_csv(output_path, encoding='utf-8', index=False)
            print(f"Encoding converted: {file_path} -> {output_path}")
        except UnicodeDecodeError:
            print(f"Skipped (encoding error): {file_path}")
        except Exception as e:
            print(f"Failed to convert {file_path}: {e}")


if __name__ == "__main__":
    # Example usage
    convert_csv_encoding('./raw_data/**/*.csv')
