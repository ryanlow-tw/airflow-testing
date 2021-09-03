import os
import logging
import glob

logging.basicConfig(filename='test.log', encoding='utf-8', level=logging.DEBUG)
SCALA_CLASS = "thoughtworks.wordcount.WordCount"
INPUT_FILE = "/Users/ryanlow/Documents/Projects/data_engineering/airflow-testing/data/words_10000sentence.txt"
OUTPUT_FILE = "/Users/ryanlow/Documents/Projects/data_engineering/airflow-testing/test-output/actual.txt"
JAR_FILE = "/Users/ryanlow/Documents/Projects/data_engineering/transformations/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar"
command= f"spark-submit --class {SCALA_CLASS} --master local {JAR_FILE} {INPUT_FILE} {OUTPUT_FILE}"

def test_that_wordcount_class_should_return_processed_txt_file():
    stream = os.popen(command)
    logging.debug(stream.read())
    actual_files = sorted(glob.glob('test-output/actual.txt/*.csv'))
    expected_files = sorted(glob.glob('test-output/expected.txt/*.csv'))

    assert len(actual_files) == len(expected_files)
    for i in range(len(actual_files)):
        with open(actual_files[i],'r') as f:
            actual_file = f.readlines()
        with open(expected_files[i], 'r') as f:
            expected_file = f.readlines()
        assert actual_file == expected_file
        