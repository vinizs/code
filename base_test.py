from customerio import CustomerIO, Regions
from datetime import datetime, timedelta
from config import *
import http.client
import codecs
import boto3
import uuid
import time
import json
import sys
import csv
import os

###############################
# Create config.py file with:
# # Customer.io
# cio_api_key = "XYZ"
# cio_site_id = "XYZ"
# cio_track_api_key = "XYZ"
# # AWS
# aws_profile = "XYZ"
###############################

try:
    # Customer.io
    cio_api_key_arg = sys.argv[1]
    cio_site_id_arg = sys.argv[2]
    cio_track_api_key_arg = sys.argv[3]
    # AWS
    aws_profile_arg = sys.argv[4]
except Exception:
    None

# Change key as required for testing
# This will be used to create the folder and read local file(without extension)
new_key = "pytest_script"

# Customer.io
cio_api_url = "api.customer.io"  # API URL
cio_uuid = str(uuid.uuid4())
cio = CustomerIO(cio_site_id, cio_track_api_key, region=Regions.US)

# AWS
bucket = "human-engagement-eu-west-1-431002633201"  # S3 bucket
path = "data/cio-data-load-automation/"  # Path for CIO automated job S3 (HUMAN-679 and HUMAN-563)
log_group_name = "/aws/lambda/human-engagement-cio-data-load"  # CloudWatch log group name
glue_job_redshift_to_s3 = "human-engagement-cio-data-load-redshift-to-s3"
glue_job_csv_to_cio = "human-engagement-cio-data-load-csv-to-customerio"
lambda_function = 'human-engagement-cio-data-load'
session = boto3.Session(profile_name=aws_profile)
s3 = session.client('s3')
glue = session.client('glue')
logs = session.client('logs')

# Date and time for naming purposes
date_time = datetime.now().strftime("_%Y-%m-%d-%H-%M-%S")

# Start time for CloudWatch logs
script_start_time = int((datetime.now()).timestamp()) * 1000

# JSON file
json_manifest_file_segment = """{
    "OPERATION_NAME": "CREATE_SEGMENTS",
    "SEGMENT_NAME": \"""" + new_key + date_time + """",
    "DESCRIPTION": \"""" + new_key + date_time + """",
    "SKIP_CIO_UPLOAD": false,
    "CREATED_BY": "HE Auto"
}"""

# New cvs file with dummy user to replace original created
new_csv_file = """client_id,id,action
33,\"""" + cio_uuid + """",identify
"""


##########################################################

# development test function. not to be used
def check_cred():
    print("test cred base")
    print(sys.argv)
    try:
        print(cio_api_key_arg)
        print(cio_site_id_arg)
        print(cio_track_api_key_arg)
        print(aws_profile_arg)
    except Exception:
        None
    return False


def check_logs():
    # Get log streams in log group
    error = 0
    log_streams_response = logs.describe_log_streams(
        logGroupName=log_group_name,
        orderBy='LastEventTime',
        descending=True
    )
    log_streams = log_streams_response['logStreams']
    # Collect log events from each log stream
    print("CloudWatch logs: ")
    for log_stream in log_streams:
        log_stream_name = log_stream['logStreamName']
        next_token = None
        while True:
            if next_token:
                response = logs.get_log_events(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name,
                    nextToken=next_token,
                    startTime=script_start_time
                )
            else:
                response = logs.get_log_events(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name,
                    startTime=script_start_time
                )
            events = response['events']
            for event in events:
                if "error" in event['message'].lower():
                    error = 1
                    print(event['message'])
                elif "OPERATION_NAME" in event['message'] and date_time in event['message']:
                    print(event['message'])
            next_token = response.get('nextToken')
            if not next_token:
                break
    if error == 0:
        return True
    else:
        return False


def check_cio():
    segment_name = new_key + date_time
    conn = http.client.HTTPSConnection(cio_api_url)
    headers = {'Authorization': "Bearer " + cio_api_key + ""}
    # Get all segments to find the right one
    conn.request("GET", "/v1/segments", headers=headers)
    res = conn.getresponse()
    data = res.read()
    # Load JSON string into Python object
    json_out = json.loads(data.decode("utf-8"))
    # Loop over the segments and find the matching ID
    result_id = None
    for segment in json_out["segments"]:
        if segment["name"] == segment_name:
            result_id = segment["id"]
            break
    # Get/delete specific segment
    if result_id is not None:
        conn.request("GET", "/v1/segments/" + str(result_id) + "/customer_count", headers=headers)
        res = conn.getresponse()
        data = res.read()
        json_out = json.loads(data.decode("utf-8"))
        if "count" in json_out:
            print(f"Segment {segment_name} created in Customer.io. Number of people: {str(json_out['count'])}.")
            conn.request("DELETE", "/v1/segments/" + str(result_id), headers=headers)
            print("Segment deleted successfully.")
            return True
        else:
            print("Key doesn't exist in JSON data")
    else:
        print("Segment not created in Customer.io")


def delete_user_cio():
    cio.delete(customer_id=str(cio_uuid))
    conn = http.client.HTTPSConnection(cio_api_url)
    headers = {'Authorization': "Bearer " + cio_api_key + ""}
    conn.request("GET", "/v1/customers?email=AutoHEMarvels@workhuman.com", headers=headers)
    res = conn.getresponse()
    data = res.read()
    timeout = time.time() + 60 * 1  # 1 min from now
    if cio_uuid not in data.decode("utf-8"):
        print(f"Dummy user deleted from Customer.io: id {cio_uuid}.")
        return True
    else:
        while cio_uuid in data.decode("utf-8"):
            conn.request("GET", "/v1/customers?email=AutoHEMarvels@workhuman.com", headers=headers)
            res = conn.getresponse()
            data = res.read()
            if cio_uuid not in data.decode("utf-8"):
                print(f"Dummy user deleted from Customer.io: id {cio_uuid}.")
                return True
            elif timeout < time.time():
                print("Deleting dummy user to Customer.io timed out...")
                return False
            else:
                time.sleep(1)


def add_user_cio():
    cio.identify(id=cio_uuid, email='AutoHEMarvels@workhuman.com', first_name='AutoHEMarvels')
    conn = http.client.HTTPSConnection(cio_api_url)
    headers = {'Authorization': "Bearer " + cio_api_key + ""}
    conn.request("GET", "/v1/customers?email=AutoHEMarvels@workhuman.com", headers=headers)
    res = conn.getresponse()
    data = res.read()
    timeout = time.time() + 60 * 1  # 1 min from now
    while cio_uuid not in data.decode("utf-8"):
        conn.request("GET", "/v1/customers?email=AutoHEMarvels@workhuman.com", headers=headers)
        res = conn.getresponse()
        data = res.read()
        if cio_uuid in data.decode("utf-8"):
            print(f"Dummy user created in Customer.io: id {cio_uuid}.")
            return True
        elif timeout < time.time():
            print("Adding dummy user to Customer.io timed out...")
            return False
        else:
            time.sleep(1)


def check_csv_lines():
    count = 0
    try:
        data = s3.get_object(Bucket=bucket, Key=path + new_key + date_time + "/" + new_key + ".csv")
        for _ in csv.DictReader(codecs.getreader("utf-8")(data["Body"])):
            count = count + 1
    except AssertionError as error:
        raise error
        # replace original csv with new one
    s3.put_object(
        Body=new_csv_file,
        Bucket=bucket,
        Key=path + new_key + date_time + '/' + new_key + '.csv'
    )
    print(f"Redshift downloaded csv file has {count} line(s).")
    return count


def check_glue(job_name):
    timeout = time.time() + 60 * 1  # X min from now to wait for the job to start
    running = 0
    succeeded = 0
    while running == 0 and timeout > time.time():
        result = glue.get_job_runs(JobName=job_name)
        for res in result['JobRuns']:
            if res.get("JobRunState") == "RUNNING":
                running = 1
                run_id = res.get("Id")
                while succeeded == 0:
                    result2 = glue.get_job_run(JobName=job_name, RunId=run_id)
                    if result2.get("JobRun").get("JobRunState") == "SUCCEEDED":
                        succeeded = 1
                        print(f"Glue job '{job_name}' succeeded.")
                        return True
                    elif result2.get("JobRun").get("JobRunState") == "FAILED" or result2.get("JobRun").get(
                            "JobRunState") == "STOPPED":
                        print("Job " + job_name + " finished with status: " +
                              result2.get("JobRun").get("JobRunState"))
                        return False
                    else:
                        time.sleep(1)
                # break
        time.sleep(1)
    print(f"Glue job '{job_name}' test timed out. It probably never started.")
    return False


def check_lambda():
    mins = 2
    current_time = datetime.now()
    x_minutes_ago = current_time - timedelta(minutes=mins)

    response = logs.filter_log_events(
        logGroupName='/aws/lambda/' + lambda_function,
        startTime=int(x_minutes_ago.timestamp() * 1000),
        endTime=int(current_time.timestamp() * 1000),
        filterPattern='REPORT'
    )

    if response['events']:
        print(f"Lambda function '{lambda_function}' was invoked successfully within the past {mins} minute(s).")
        return True
    else:
        print(f"Lambda function '{lambda_function}' was NOT invoked successfully within the past {mins} minute(s).")
        return False


def delete_folder(full_path):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=full_path)
    if 'Contents' in response:
        # Delete each object within the folder, including folder
        for obj in response['Contents']:
            s3.delete_object(Bucket=bucket, Key=obj['Key'])
    else:
        print("No keys to be deleted.")
        return False

    response = s3.list_objects_v2(Bucket=bucket, Prefix=full_path)
    if 'Contents' in response:
        print("Something went wrong while deleting S3 files")
        return False
    else:
        print("S3 uploaded files deleted after test.")
        return True


def check_file_created(file_ext):
    try:
        _ = s3.get_object(Bucket=bucket, Key=path + new_key + date_time + "/" + new_key + file_ext)
        return True
    except AssertionError as error:
        raise error


def get_file_path(file_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'files', file_name)
    return file_path


def upload_file(file_ext, file_content):
    # create folder
    s3.put_object(Bucket=bucket, Key=(path + new_key + date_time + '/'))
    # upload file
    if file_ext == ".sql":
        if os.path.exists(get_file_path("script" + file_ext)):
            s3.upload_file(
                Filename=get_file_path("script" + file_ext),
                Bucket=bucket,
                Key=path + new_key + date_time + '/' + new_key + file_ext
            )
        else:
            raise Exception(f"File {new_key}{file_ext} does not exist locally.")
    else:
        s3.put_object(
            Body=file_content,
            Bucket=bucket,
            Key=path + new_key + date_time + '/' + new_key + file_ext
        )
    if check_file_created(file_ext):
        return True


# main
if __name__ == '__main__':
    None
    check_cred()
