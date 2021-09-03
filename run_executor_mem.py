import os
import logging
from datetime import datetime
import time

now = datetime.now()
filename = f'logs/airflow-logs{now}-{now.hour}-{now.minute}-{now.second}'
logging.basicConfig(filename=filename, encoding='utf-8', level=logging.DEBUG)

def get_airflow_run_duration(num_runs, filesize, num_executors, executor_mem):
    duration = 0
    for i in range(num_runs):
        print(f"This is run number {i+1}")
        run_bash_command(f"airflow variables set filesize {filesize}")
        run_bash_command(f"airflow variables set num_executors {num_executors}")
        run_bash_command(f"airflow variables set executor_mem {executor_mem}")
        start = time.time()
        run_bash_command("airflow tasks test wordcount_dag count_words_task 2021-08-30")
        end = time.time()
        duration += (end - start)
    return round((duration / num_runs), 2)

def write_file(input_1, input_2, input_3, input_4):
    with open('results_executor_mem.csv', 'a') as f:
        f.write(f"{input_1},{input_2},{input_3},{input_4}\n")

def run_bash_command(command):
    stream = os.popen(command)
    logging.debug(stream.read())

def main():
    filesize = 1000000
    NUM_RUNS = 3
    executor = 4
    executor_mem = ["20M", "50M", "100M", "1G", "4G"]
    write_file(input_1="size", input_2="average_time", input_3="num_executors", input_4="executor_mem")
    for mem in executor_mem:
        average_time = get_airflow_run_duration(
            num_runs=NUM_RUNS,
            filesize=filesize,
            num_executors=executor,
            executor_mem=mem)
        print(f"""
Time taken for wordcount,
filesize: {filesize}
with num executors {executor}
is {average_time:.2f} seconds""")
        write_file(input_1=filesize, input_2=average_time, input_3=executor, input_4=mem)

if __name__ == "__main__":
    main()
    