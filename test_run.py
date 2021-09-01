import time
import pytest
from run import get_airflow_run_duration


def test_that_100MB_wordcount_job_should_not_be_more_than_45_seconds():
    duration = get_airflow_run_duration(num_runs=1, size="100MB")
    assert duration <= 45
    