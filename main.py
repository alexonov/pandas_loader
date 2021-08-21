import pandas as pd
from pathlib import Path
from typing import Iterable, Union

DATA_FOLDER = Path().parent / 'data'

OUTPUT_FILENAME = 'output.dat'

INPUT_COLUMN_NAMES = (
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

OUTPUT_COLUMN_NAMES = [
    'date_column',
    'value_1',
    'value_2'
]


def load_files_generator() -> Iterable[pd.DataFrame]:
    for file_path in DATA_FOLDER.iterdir():
        if file_path.suffix == '.dat':
            yield parse_file_to_frame(file_path)


def parse_file_to_frame(file_path: Union[str, Path]) -> pd.DataFrame:
    file_df = pd.read_csv(
        file_path,
        header=None,
        delim_whitespace=True,
        names=INPUT_COLUMN_NAMES
    )
    file_df['date_column'] = pd.to_datetime(file_df['date_column'], format="%Y-%m-%d/%H:%M:%S:%f")
    file_df.sort_values(
        SORTING_COLUMNS,
        inplace=True,
        ascending=True
    )
    return file_df


def process_frame(df: pd.DataFrame) -> pd.DataFrame:
    data = df.values.tolist()
    result = []
    for line in data:

        # TODO: add your logic here
        r = [
            line[0],
            line[1] + line[2],
            line[3] - line[4]
        ]

        result.append(r)

    return pd.DataFrame.from_records(result, columns=OUTPUT_COLUMN_NAMES)


if __name__ == '__main__':
    processed_dfs = []
    for df in load_files_generator():
        processed_dfs.append(process_frame(df))

    final_df = pd.concat(processed_dfs)
    final_df.to_csv(Path().parent / OUTPUT_FILENAME, index=False)

    exit(0)
