from datetime import datetime
from io import StringIO

import pandas as pd
import requests
import sqlalchemy
from openhexa.sdk import current_run, parameter, pipeline, workspace


@pipeline("brussels_bikes", name="Bikes in Brussels")
@parameter(
    "start_date",
    required=True,
    type=str,
    default="20230501",
    help="Start date of the history (YYYYMMDD)",
)
@parameter(
    "end_date",
    required=True,
    type=str,
    default="20230601",
    help="Start date of the history (YYYYMMDD)",
)
@parameter(
    "device_id",
    required=True,
    type=str,
    default="CB1142",
    help="Device ID of the counter (Ex: CB1142)",
)
@parameter("int_list", required=False, type=int, multiple=True, help="List of integers")
def bikes(start_date, end_date, device_id, int_list):
    current_run.log_info("Starting pipeline.")
    current_run.log_info(f"int_list:{int_list}")
    devices = load_devices()
    save_devices(devices)
    load_history(start_date, end_date, device_id)
    current_run.log_info("Done !")


@bikes.task
def load_history(start_date, end_date, device_id):
    csv_data = requests.get(
        f"https://data.mobility.brussels/bike/api/counts/?request=history&outputFormat=csv&featureID={device_id}&startDate={start_date}&endDate={end_date}"
    ).content

    ds = pd.read_csv(StringIO(csv_data.decode("utf-8")))
    con = sqlalchemy.create_engine(workspace.database_url)
    ds.to_sql("history", con=con, if_exists="append")
    current_run.add_database_output("history")
    path = f"{workspace.files_path}/history_{datetime.now()}.csv"
    ds.to_csv(path)
    current_run.add_file_output(path)


@bikes.task
def load_devices():
    current_run.log_info("Get devices...")

    data = requests.get(
        "https://data.mobility.brussels/bike/api/counts/?request=devices"
    ).json()["features"]

    return pd.DataFrame.from_dict(data)


@bikes.task
def save_devices(devices):
    current_run.log_info("Save devices...")

    devices.to_csv(f"{workspace.files_path}/devices_{datetime.now()}.csv", index=False)


if __name__ == "__main__":
    bikes()
