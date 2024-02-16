"""Template for newly generated pipelines."""

import os.path
from pathlib import Path

import papermill as pm
from openhexa.sdk import current_run, pipeline, workspace


@pipeline("where-is-fred", name="Where is Fred? 🚴‍♂️")
def where_is_fred():
    run_notebook()


@where_is_fred.task
def run_notebook():
    current_run.log_info("Running notebook...")
    pm.execute_notebook(
        Path(workspace.files_path) / "notebooks/WhereIsFred.ipynb",
        Path(workspace.files_path) / "notebooks/outputs/WhereIsFred.ipynb",
        parameters=dict(),
        request_save_on_cell_execute=False,
        progress_bar=False,
    )


if __name__ == "__main__":
    where_is_fred()
