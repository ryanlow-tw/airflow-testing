import os
import logging
from datetime import datetime
import time

now = datetime.now()
filename = f'logs/airflow-logs{now}-{now.hour}-{now.minute}-{now.second}'
logging.basicConfig(filename=filename, encoding='utf-8', level=logging.DEBUG)

def get_airflow_run_duration(num_runs, size):
    start = time.time()
    for i in range(num_runs):
        print(f"This is run number {i+1}")
        bash_command = f"airflow tasks test wordcount_{size}_dag count_words_{size}_task 2021-08-30"
        stream = os.popen(bash_command)
        logging.debug(stream.read())
    end = time.time()
    return end - start

def main():
    filesizes = ["1MB", "5MB", "10MB", "20MB","100MB"]
    NUM_RUNS = 3
    with open('results.csv', 'a') as f:
        f.write("size,average_time\n")
    for size in filesizes:
        average_time = get_airflow_run_duration(num_runs=NUM_RUNS, size=size)
        print(f"Time taken for wordcount, filesize: {size} is {average_time:.2f} seconds")
        with open('results.csv', 'a') as f:
            size_without_mb = size[:-2]
            f.write(f"{size_without_mb},{average_time}\n")


if __name__ == "__main__":
    main()
    