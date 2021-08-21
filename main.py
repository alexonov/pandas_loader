import pandas as pd
from pathlib import Path
from typing import Iterable, Union

DATA_FOLDER = Path().parent / 'data'
COLUMN_NAMES = (
    'date_column',
    'small_int_column',
    'small_float_column',
    'large_int_column',
    'large_float_column'
)

SORTING_COLUMNS = [
    'date_column',
    'large_int_column',
    'small_int_column',
]


def load_files_generator() -> Iterable[pd.DataFrame]:
    for file_path in DATA_FOLDER.iterdir():
        yield parse_file_to_frame(file_path)


def parse_file_to_frame(file_path: Union[str, Path]) -> pd.DataFrame:
    file_df = pd.read_csv(
        file_path,
        header=None,
        delim_whitespace=True,
        names=COLUMN_NAMES
    )
    file_df['date_column'] = pd.to_datetime(file_df['date_column'], format="%Y-%m-%d/%H:%M:%S:%f")
    file_df.sort_values(
        SORTING_COLUMNS,
        inplace=True,
        ascending=True
    )
    return file_df


if __name__ == '__main__':
    for df in load_files_generator():
        print(df.head())

    exit(0)
