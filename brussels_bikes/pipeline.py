from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
import sqlalchemy
from openhexa.sdk.pipelines import current_run, pipeline
from openhexa.sdk.workspaces import workspace


def fetch_history(device_id, start_date, end_date):
    csv_data = requests.get(
        f"https://data.mobility.brussels/bike/api/counts/?request=history&outputFormat=csv&featureID={device_id}&startDate={start_date.strftime('%Y%m%d')}&endDate={end_date.strftime('%Y%m%d')}"
    ).content

    return pd.read_csv(StringIO(csv_data.decode("utf-8")))


@pipeline("brussels_bikes", name="Bikes in Brussels")
def bikes():
    current_run.log_info("Starting pipeline...")
    devices = load_devices()
    history = load_history(devices)

    save_dataset(devices, history)


@bikes.task
def save_dataset(devices, history):
    dataset = workspace.get_dataset("bikes-in-brussels-6f971d")
    version = dataset.create_version(datetime.now())
    version.add_file(StringIO(devices.to_csv(index=False)), "devices.csv")
    version.add_file(StringIO(history.to_csv(index=False)), "history.csv")


@bikes.task
def load_history(devices):
    now = datetime.now()
    current_run.log_info(
        f"Load history from {now - timedelta(days=2)} to {now - timedelta(days=1)}..."
    )

    history = pd.DataFrame()
    for row in devices.itertuples(index=True, name="Device"):
        current_run.log_debug("Load history for device: " + row.id)
        device_history = fetch_history(
            row.id, now - timedelta(days=2), now - timedelta(days=1)
        )
        device_history["device_id"] = row.id
        history = pd.concat([history, device_history], axis=0)
    current_run.log_debug(f"Database: {workspace.database_url}")
    con = sqlalchemy.create_engine(workspace.database_url)
    history.to_sql("bikes_history", con=con, if_exists="append")
    current_run.add_database_output("history")

    path = f"{workspace.files_path}/history_{now}.csv"
    history.to_csv(path)
    current_run.add_file_output(path)

    return history


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
