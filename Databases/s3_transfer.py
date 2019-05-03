from datetime import datetime
import boto3
import os
import json
import traceback
import pandas as pd
from functools import wraps
from time import time


def s3_transfer(bucket_name, file_name, direction, file_type, file_to_upload=None):
    """
    This function uploads or downloads files from AWS s3 buckets depending on
    user inputs

    Params:

    bucket_name (str),
    file_name (str),
    file_to_upload (default: None type, accepts any file ending)
    direction (str), only accepts "upload" or "download"

    """
    s3_resource = boto3.resource('s3')
    key = '{}.{}'.format(file_name, file_type)
    if direction.lower() == 'upload':
        try:
            s3_resource.Bucket(name=bucket_name).upload_file(
                Filename=file_to_upload, Key=key)
        except:
            print("\nupload error occured at s3 bucket block with error \
            message:\n")
            traceback.print_exc(limit=1)
    elif direction.lower() == 'download':
        try:
            content = s3_resource.Object(bucket_name, key).get()[
                'Body'].read().decode('utf-8')
            if file_type.lower() == 'csv':
                return pd.read_csv(content)
            elif file_type.lower() == 'json':
                return json.loads(content)
        except:
            print("download error occured at s3 bucket block with error \
            message:")
            traceback.print_exc(limit=1)
    else:
        print("direction entered can only be upload or download! you entered: {0}".format(
            direction))
