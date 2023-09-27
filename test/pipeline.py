from openhexa.sdk import current_run, pipeline


@pipeline("test", name="test")
def test():
    count = task_1()
    task_2(count)


@test.task
def task_1():
    current_run.log_info("In task 1...")

    return 42


@test.task
def task_2(count):
    current_run.log_info(f"In task 2... count is {count}")


if __name__ == "__main__":
    test()
