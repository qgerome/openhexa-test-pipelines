from datetime import datetime, timedelta
from io import BytesIO, StringIO

import pandas as pd
import requests
import sqlalchemy
from openhexa.sdk.pipelines import current_run, parameter, pipeline
from openhexa.sdk.workspaces.connection import PostgreSQLConnection
from openhexa.sdk.workspaces.workspace import Dataset, workspace


def fetch_history(device_id, start_date, end_date):
    csv_data = requests.get(
        f"https://data.mobility.brussels/bike/api/counts/?request=history&outputFormat=csv&featureID={device_id}&startDate={start_date}&endDate={end_date}"
    ).content

    return pd.read_csv(StringIO(csv_data.decode("utf-8")))


@pipeline("brussels_bikes", name="Bikes in Brussels")
@parameter("dataset", name="Dataset", type=Dataset, required=True)
@parameter("save_in_db", name="Save in DB", type=bool, default=False)
@parameter(
    "start_date", name="Start date", type=str, help="Format: YYYYMMDD", required=False
)
@parameter(
    "end_date",
    name="End date",
    type=str,
    help="Format: YYYYMMDD. By default, Yesterday",
    required=False,
)
@parameter(
    "version_name",
    name="Version name",
    type=str,
    help="By default: now",
    required=False,
)
@parameter("n_days", name="Number of days", type=int, required=False)
def bikes(dataset, save_in_db, start_date, end_date, version_name, n_days):
    current_run.log_info("Starting pipeline...")
    devices = load_devices()
    history = load_history(devices, start_date, end_date)

    save_dataset(version_name, devices, history, dataset)
    if save_in_db:
        save_db(devices, history)


@bikes.task
def save_dataset(version_name, devices, history, dataset):
    version_name = version_name or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    version = dataset.create_version(version_name)
    current_run.log_info(f"Save dataset: {dataset.slug} - {version_name}")

    current_run.log_info("Save devices.csv and history.csv")

    # Since we want to save a dataframe, we need to convert it to a csv file and
    # then convert it to a stream of bytes
    version.add_file(
        BytesIO(devices.to_csv(index=False).encode("utf-8")), "devices.csv"
    )


@bikes.task
def load_history(devices, start_date):
    now = datetime.now()
    if start_date:
        start_date = datetime.strptime(start_date, "%Y%m%d")
    else:
        start_date = now - timedelta(days=2)
    current_run.log_info(
        f"Load history from {start_date} to {now - timedelta(days=1)}..."
    )


@bikes.task
def save_db(devices, history):
    current_run.log_info("Save in database...")
    con = sqlalchemy.create_engine(workspace.database_url)
    devices.to_sql("bikes_devices", con=con, if_exists="replace")
    history.to_sql("bikes_history", con=con, if_exists="replace")
    current_run.add_database_output("bikes_devices")
    current_run.add_database_output("bikes_history")


@bikes.task
def load_devices():
    current_run.log_info("Load devices...")

    data = requests.get(
        "https://data.mobility.brussels/bike/api/counts/?request=devices"
    ).json()["features"]

    return pd.DataFrame.from_dict(
        [
            {
                "id": d["id"].split(".")[-1],
                "coordinates": d["geometry"]["coordinates"],
                **d["properties"],
            }
            for d in data
        ]
    )


if __name__ == "__main__":
    bikes()
