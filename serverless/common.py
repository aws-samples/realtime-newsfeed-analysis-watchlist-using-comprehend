# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json
from datetime import datetime
import botocore.exceptions
import logging
log = logging.getLogger()
log.setLevel(logging.INFO)


def save_content_to_bucket(bucket, sub_dir, filename, suffix, content, content_type="TEXT"):
    """
    Save a content to a bucket
    :param bucket: the bucket
    :param sub_dir: prefix or subdirectory within the bucket
    :param filename: the filename to save the content
    :param suffix: append suffix to the filename
    :param content: the data to save
    :param content_type: Type of content provided - JSON/TEXT. default is TEXT
    :return: True
    """
    try:
        s3 = boto3.resource('s3')
        filepath = sub_dir + '/' + filename + suffix
        log.info("Writing file {0}".format(filepath))

        if content_type == "TEXT":
            s3.Object(bucket, filepath).put(Body=content)
        else:
            s3.Object(bucket, filepath).put(Body=json.dumps(content))
    except Exception as e:
        log.error("Exception in save_content_to_bucket", e)
        return False
    return True


def limited_text(input_text, size):
    """
    limited text, the function checks if the input text exceeds 5000 bytes, and if so, returns the first 5000 characters.
    """
    result = ""
    if len(input_text.encode('utf-8')) > size:
        for i in input_text:
            if len((result + i).encode('utf-8')) < size:
                result = result + i
        log.info(len(result))
    else:
        result = input_text
    log.info(len(result.encode('utf-8')))
    return result


def get_secret(secret_name):
    """
        Get the secret dictionary - key/value
    :param secret_name: the name of the secret
    :return: the secret dictionary
    """
    region_name = "us-east-2"
    secret = None
    # Create a Secrets Manager client
    session = boto3.session.Session()
    # print("before secret")
    # endpoint_url = "https://secretsmanager.us-east-2.amazonaws.com"
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
        # endpoint_url=endpoint_url
    )
    # endpoint_url='https://secretsmanager.us-west-2.amazonaws.com'

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    except Exception as e:
        raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    # print("after secret")
    return json.loads(secret)  # returns the secret as dictionary
