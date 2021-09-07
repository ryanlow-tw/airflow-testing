import glob
import os
from shutil import rmtree
from datetime import datetime, timedelta
import pytest
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

OUTPUT_PATH = "test-output/actual/"

DEFAULT_ARGS = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2021, 8, 30),
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }


@pytest.fixture(scope='module', autouse=True)
def fixture():
    if os.path.exists(OUTPUT_PATH):
        rmtree(OUTPUT_PATH, ignore_errors=False)
    dag = DAG('test_wordcount_dag', default_args=DEFAULT_ARGS)
    t1 = TriggerDagRunOperator(task_id='trigger_word_count_dag',
                                        trigger_dag_id='test_wordcount_dag',
                                        wait_for_completion=True,
                                        trigger_rule=TriggerRule.ONE_SUCCESS,
                                        dag=dag,
                                        poke_interval=5)
    t1.execute(context={})

def test_that_calling_airflow_dag_should_return_text_file_with_word_and_count():
    actual_files = sorted(glob.glob(f'{OUTPUT_PATH}*.csv'))
    expected_files = sorted(glob.glob('test-output/expected/*.csv'))

    assert len(actual_files) == len(expected_files)
    for i in range(len(actual_files)):
        with open(actual_files[i],'r') as f:
            actual_file = f.readlines()
        with open(expected_files[i], 'r') as f:
            expected_file = f.readlines()
        assert actual_file == expected_file

def test_that_SUCCESS_file_is_in_output_folder():

    SUCCESS_FILE = glob.glob(f"{OUTPUT_PATH}_SUCCESS")

    assert SUCCESS_FILE == [f"{OUTPUT_PATH}_SUCCESS"]
    