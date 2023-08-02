import pytest
from base_test import *


def test_lambda():
    if upload_file('.json', json_manifest_file_segment):
        #  if upload_file('.csv', new_csv_file):
        if upload_file('.sql', None):
            time.sleep(60)  # it takes some time for the lambda function to be triggered by the uploaded S3 file.
            assert check_lambda()
        else:
            AssertionError
    else:
        AssertionError


def test_glue_redshift_to_s3():
    assert check_glue(glue_job_redshift_to_s3)


def test_check_csv_lines():
    assert check_csv_lines() == 1


def test_add_user_cio():
    assert add_user_cio()


def test_glue_csv_to_cio():
    time.sleep(15)
    assert check_glue(glue_job_csv_to_cio)


def test_check_cio():
    assert check_cio()


def test_delete_user_cio():
    assert delete_user_cio()


def test_check_logs():
    assert check_logs()


# Should be last test
def test_delete_folder():
    assert delete_folder(path+new_key+date_time)
