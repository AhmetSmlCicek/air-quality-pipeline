from src.extract_azure import get_data,uploadFile_to_azure
from src.stream_azure_blob import dataStream_in_deltaTable
from prefect import flow,task
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import LocalFileSystem
from prefect.blocks.system import String


# Define the tasks using the `@task` decorator
@task
def task1():
    return get_data()

@task
def task2():
    return uploadFile_to_azure()

@task
def task3():
    return dataStream_in_deltaTable()

# Create the Prefect flow
@flow(name="flow_with_dependencies")
def main_flow():
    # Define the tasks and their dependencies
    t1 = task1()
    t2 = task2(wait_for=[t1])
    t3 = task3(wait_for=[t2])

if __name__=='__main__':
    main_flow()
