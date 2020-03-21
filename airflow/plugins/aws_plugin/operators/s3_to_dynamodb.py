#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
This module contains operators to replicate records from
DynamoDB table to S3.
"""

from copy import copy
from os.path import getsize
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict, Optional
from uuid import uuid4

import boto3
import json

from airflow.models import BaseOperator
#from airflow.providers.amazon.aws.hooks.aws_dynamodb import AwsDynamoDBHook
from airflow.hooks.S3_hook import S3Hook


def _convert_item_to_json_bytes(item):
    return (json.dumps(item) + '\n').encode('utf-8')


def _upload_file_to_s3(file_obj, bucket_name, s3_key_prefix):
    s3_client = S3Hook().get_conn()
    file_obj.seek(0)
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class S3ToDynamoDBOperator(BaseOperator):
    """
    Replicates records from S3 to a DynamoDB table.
    It scans S3 for a data file and write the records to a table in DynamoDB

    :param s3_bucket_name: S3 bucket to replicate data from
    :param dynamodb_table_name: Dynamodb table to replicate data to
    :param s3_key_prefix: Prefix of s3 object key
    """

    def __init__(self,
                 dynamodb_table_name: str,
                 s3_bucket_name: str,
                 s3_key_prefix: str = '',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamodb_table_name = dynamodb_table_name
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix

    def execute(self, context):
        return True
    #     table = AwsDynamoDBHook().get_conn().Table(self.dynamodb_table_name)
    #     scan_kwargs = copy(self.dynamodb_scan_kwargs) if self.dynamodb_scan_kwargs else {}
    #     err = None
    #     f = NamedTemporaryFile()
    #     try:
    #         f = self._scan_dynamodb_and_upload_to_s3(f, scan_kwargs, table)
    #     except Exception as e:
    #         err = e
    #         raise e
    #     finally:
    #         if err is None:
    #             _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
    #         f.close()

    # def _scan_dynamodb_and_upload_to_s3(self, temp_file, scan_kwargs, table):
    #     while True:
    #         response = table.scan(**scan_kwargs)
    #         items = response['Items']
    #         for item in items:
    #             temp_file.write(self.process_func(item))

    #         if 'LastEvaluatedKey' not in response:
    #             # no more items to scan
    #             break

    #         last_evaluated_key = response['LastEvaluatedKey']
    #         scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

    #         # Upload the file to S3 if reach file size limit
    #         if getsize(temp_file.name) >= self.file_size:
    #             _upload_file_to_s3(temp_file, self.s3_bucket_name,
    #                                self.s3_key_prefix)
    #             temp_file.close()
    #             temp_file = NamedTemporaryFile()
    #     return temp_file