import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

def merge_load_factor(load_factor_file, df):
    return df.merge(load_load_factor(load_factor_file),
                  left_on=['Origin','Destination','Flight Number', 'Departure Date'],
                  right_on = ['Origin','Destination','Flight Number', 'Departure Date'],
                  how='left')

def load_load_factor(filename):
    return pd.read_csv(filename, sep=",")

def load_origin_destination(filename):
    od = pd.read_csv(filename, sep=",")
    od = od.replace(to_replace="SXF", value="BER")
    od = od[od["GKA"] != "\\N"]
    return od

def load_all_datasets(list_of_files):
    all_dataframes = []
    for filename in list_of_files:
        if filename.suffix == ".csv":
            print("Reading {}".format(filename))
            df = pd.read_csv(filename, sep=",")
            all_dataframes.append(df)
    return pd.concat(all_dataframes)

def convert_datetimes_to_numeric(df):
    df["Departure Time"] = df.apply(lambda row: row["Departure Time"].replace(":", "."), axis=1)
    df["Departure Date"] = df["Departure Date"].apply(pd.to_datetime)
    df["Departure Date"] = df["Departure Date"].values.astype(np.float64) / 10 ** 9

    #NEW Capture_date
    df["Capture Date"] = df["Capture Date"].apply(pd.to_datetime)
    df["Capture Date"] = df["Capture Date"].values.astype(np.float64) / 10 ** 9
    return df

def get_coordinates(args):
    origin_destination = load_origin_destination(args.origin_destination)
    airport_codes = origin_destination["GKA"]
    coordinates = origin_destination[origin_destination.columns[6:8]]
    coordinates = pd.concat([airport_codes, coordinates], axis=1)
    coordinates = coordinates.rename(
        {"GKA": "Origin", "-6.081689834590001": "o_altitude", "145.391998291": "o_longitude"}, axis=1)
    coordinates.set_index("Origin")
    return coordinates

def dump_clean_dataset(df):
    df.to_csv("dataset/final_dataset_train.csv")

def run(args):
    coordinates = get_coordinates(args)
    final_datasets = Path(args.finaldata_train)
    if final_datasets.is_dir():
        df = load_all_datasets(final_datasets.iterdir())
        df = merge_load_factor(args.load_factor, df)
        df = convert_datetimes_to_numeric(df)
        df = pd.merge(df, coordinates, on=['Origin'])
        coordinates = coordinates.rename({"Origin": "Destination", "o_altitude": "d_altitude", "o_longitude": "d_longitude"}, axis=1)
        df = pd.merge(df, coordinates, on=['Destination'])
        cols = df.columns.tolist()
        df = df[cols[-4:] + cols[2:-4]].apply(pd.to_numeric)
        df = df.astype('int64')
        dump_clean_dataset(df)
        print(df.head(10))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data Preprocessing')
    parser.add_argument('finaldata_train',
                    help='Path to the input directory or files')
    parser.add_argument('-od', '--origin_destination',
                    help='name for the origin destination file', default ="dataset/origin_destination.csv")
    parser.add_argument('-lf', '--load_factor',
                        help='name for the load factor file', default="dataset/finallf_train.csv")

    args = parser.parse_args(sys.argv[1:])
    run(args)

