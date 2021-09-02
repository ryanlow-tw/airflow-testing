import os
import logging
from datetime import datetime
import time

now = datetime.now()
filename = f'logs/airflow-logs{now}-{now.hour}-{now.minute}-{now.second}'
logging.basicConfig(filename=filename, encoding='utf-8', level=logging.DEBUG)

def get_airflow_run_duration(num_runs, filesize):
    duration = 0
    for i in range(num_runs):
        print(f"This is run number {i+1}")
        run_bash_command(f"airflow variables set filesize {filesize}")
        start = time.time()
        run_bash_command("airflow tasks test wordcount_dag count_words_task 2021-08-30")
        end = time.time()
        duration += (end - start)
    return round((duration / num_runs), 2)

def write_file(input_1, input_2):
    with open('results.csv', 'a') as f:
        f.write(f"{input_1},{input_2}\n")

def run_bash_command(command):
    stream = os.popen(command)
    logging.debug(stream.read())

def main():
    filesizes = [10000,20000,50000,100000,200000,500000,1000000,2000000,5000000,10000000]
    NUM_RUNS = 3
    write_file(input_1="size", input_2="average_time")
    for filesize in filesizes:
        average_time = get_airflow_run_duration(num_runs=NUM_RUNS, filesize=filesize)
        print(f"Time taken for wordcount, filesize: {filesize} is {average_time:.2f} seconds")
        write_file(input_1=filesize, input_2=average_time)

if __name__ == "__main__":
    main()
    