#!/usr/bin/python

# This script alows you to download all the files from a DigitalOcean Space
# It is based on the code from https://www.digitalocean.com/community/tutorials/how-to-manage-digitalocean-spaces-with-boto3-to-remotely-store-and-retrieve-data
# It is necessary to install boto3 library

# pip3 install boto3

import boto3
from botocore.client import Config

# Configure boto client with DigitalOcean Spaces credentials
access_key = 'sample_key'
secret_key = 'sample_sk'
region = 'fra1'  # This is the region of your Space
endpoint_url = f'https://{region}.digitaloceanspaces.com'
final_dir = '/Users/my_user/Downloads/volume_content'  # This is the directory where the files will be downloaded

# Initialize boto3 client to use Spaces
s3 = boto3.client('s3', endpoint_url=endpoint_url, aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key, config=Config(signature_version='s3v4'))

# Download all files from a Space


def do_space_file_downloader(space_name, prefix=''):
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=space_name, Prefix=prefix)

    accumulator = 0

    for response in response_iterator:
        for obj in response.get('Contents', []):
            key = obj['Key']

            if key.endswith('/'):  # Is a directory
                # Download files from subdirectory recursively
                print(f'Downloading from directory: {key}')
                do_space_file_downloader(space_name, prefix=key)
            else:
                # Download file
                file_name = key.split('/')[-1]
                file_path = f'{final_dir}/{file_name}'
                s3.download_file(space_name, key, file_path)
                accumulator += 1
                print(f'Downloading file {accumulator} to: {file_path}')


# Call function to download all files from a Space
space_name = 'mbbdd'
do_space_file_downloader(space_name)
