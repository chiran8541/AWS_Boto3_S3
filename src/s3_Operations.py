import json
import os
import pprint
import sys
import threading

import boto3
from boto3.s3.transfer import TransferConfig

BUCKET_NAME = 'chiran-s3-2021-bucket'
WEBSITE_BUCKET_NAME = 'myowntesting'
def s3_client():
    s3 = boto3.client('s3')
    """ :type : pyboto3.s3 """
    return s3

def s3_resource():
    s3 = boto3.resource('s3')
    """ :type : pyboto3.s3 """
    return s3

# creating a bucket in s3
def create_bucket(bkt):
    return s3_client().create_bucket(
        Bucket=bkt,
        # CreateBucketConfiguration={
        #     'LocationConstraint': 'us-east-1'
        # }
    )


# creating the s3 bucket policy
def create_bucket_policy():
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'AddPerm',
            'Effect': 'Allow',
            'Principal': '*',
            'Action': ['s3:*'],
            'Resource': 'arn:aws:s3:::chiran-s3-2021-bucket/*'
        }]
    }
    # convert the policy from json dict to string
    policy_string = json.dumps(bucket_policy)

    # set the new policy
    return s3_client().put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=policy_string
    )


# getting list of existing buckets
def list_of_buckets():
    response = s3_client().list_buckets()
    print('Existing buckets..')
    for bucket in response['Buckets']:
        print(f'Name : {bucket["Name"]}')


# get the properties of existing bucket
def list_of_bucket_policy():
    response = s3_client().get_bucket_policy(Bucket=BUCKET_NAME)
    print(str(response['Policy']))


# similarly you can get the list of other properties by using s3_client.get method

# lets update the bucket policy
def update_bucket_policy(bucket_name):
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AddPerm',
                'Effect': 'Allow',
                'Principal': '*',
                'Action': ['s3:DeleteObject', 's3:GetObject', 's3:PutObject'],
                'Resource': 'arn:aws:s3:::' + bucket_name + '/*'
            }
        ]
    }
    # convert the policy from json dict to string
    policy_string = json.dumps(bucket_policy)

    # set the new policy
    return s3_client().put_bucket_policy(
        Bucket=bucket_name,
        Policy=policy_string
    )

def server_side_encryption():
    return s3_client().put_bucket_encryption(
        Bucket=BUCKET_NAME,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }
            ]
        }
    )

def delete_bucket(bucket_name):
    print(f'Deleting the bucket with name : {BUCKET_NAME}')
    return s3_client().delete_bucket(Bucket=bucket_name)

def upload_small_file():
    file_path = os.path.dirname(__file__) + '/readme.txt'
    return s3_client().upload_file(file_path, BUCKET_NAME, 'readme.txt')

def upload_large_file():
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path = os.path.dirname(__file__) + '/Linux.docx'
    key_path = 'multipart_files/Linux.docs'
    return s3_resource().meta.client.upload_file(file_path, BUCKET_NAME, key_path,
                              ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/docs'},
                              Config=config,
                              Callback=ProgressPercentage(file_path) )

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self.lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self.lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s %s / %s (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size, percentage
                )
            )
            sys.stdout.flush()
#*****************************************************

def read_object_from_bucket():
    obj_key= 'readme.txt'
    return s3_client().get_object(Bucket=BUCKET_NAME, Key=obj_key)

def version_bucket_files():
    s3_client().put_bucket_versioning(
        Bucket=BUCKET_NAME,
        VersioningConfiguration={
            'Status': 'Enabled'
                }
        )

def upload_new_version():
    file_path = os.path.dirname(__file__) + '/readme.txt'
    return s3_client().upload_file(file_path, BUCKET_NAME, 'readme.txt')

def put_lifecycle_policy():
    lifecycle_policy = {
        "Rules": [
            {
                "ID": "Move readme file to Glacier",
                "Prefix": "readme",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Date": "2021-01-10T00:00:00.000Z",
                        "StorageClass": "GLACIER"
                    }
                ]
            },
            {
                "Status": "Enabled",
                "Prefix": "",
                "NoncurrentVersionTransitions": [
                    {
                        "NoncurrentDays": 2,
                        "StorageClass": "GLACIER"
                    }
                ],
                "ID": "Move old versions to Glacier"
            }
        ]
    }
    s3_client().put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration=lifecycle_policy
    )

def host_static_website():
    s3 = boto3.client('s3')
    """ :type : pyboto3.s3 """

    s3.create_bucket(Bucket=WEBSITE_BUCKET_NAME)
    update_bucket_policy(WEBSITE_BUCKET_NAME)

    website_configuration ={
        'ErrorDocument': {'Key': 'error.html'},
        'IndexDocument': {'Suffix': 'index.html'}
    }
    s3.put_bucket_website(
        Bucket=WEBSITE_BUCKET_NAME, WebsiteConfiguration=website_configuration
    )

    index_file = os.path.dirname(__file__) + "/index.html"
    error_file = os.path.dirname(__file__) + "/error.html"

    s3_client().put_object(Bucket=WEBSITE_BUCKET_NAME, ACL='public-read', Key='index.html',
                           Body=open(index_file).read(), ContentType='text/html')
    s3_client().put_object(Bucket=WEBSITE_BUCKET_NAME, ACL='public-read', Key='error.html',
                           Body=open(error_file).read(), ContentType='text/html')


if __name__ == '__main__':
    # print(create_bucket(BUCKET_NAME))
    # print(create_bucket_policy())
    #list_of_buckets()
    #list_of_bucket_policy()
    #print(update_bucket_policy(BUCKET_NAME))
    #print(server_side_encryption()
    print(delete_bucket(BUCKET_NAME))
    # print(upload_small_file())
    # print(upload_large_file())
    #print(read_object_from_bucket())
    #version_bucket_files()
    #upload_new_version()
    # put_lifecycle_policy()
    #host_static_website()
