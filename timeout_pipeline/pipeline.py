from time import sleep

from openhexa.sdk import current_run, pipeline


@pipeline("timeout-pipeline", name="Kill by timeout")
def timeout_pipeline():
    task_1()


@timeout_pipeline.task
def task_1():
    current_run.log_info("In task 1...")
    index = 0
    while True:
        current_run.log_info(f"Waited 1 second. {index}s passed")
        sleep(1)
        index += 1


if __name__ == "__main__":
    timeout_pipeline()
