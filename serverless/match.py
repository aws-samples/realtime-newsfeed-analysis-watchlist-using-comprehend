# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import common
import os
import json
import logging
log = logging.getLogger()
log.setLevel(logging.INFO)


def detect_watchlist(content):
    try:
        config = common.get_secret(os.environ['SECRET'])
        client = boto3.client('sns')
        print("Publishing a Match")
        response = client.publish(
            TopicArn=config['sns-notification-topic'],
            Message=json.dumps(
                {
                    'default': json.dumps(content)
                 }),
            Subject='Watchlist Matched!',
            MessageStructure='json'
        )

    except Exception as e:
        print("Error executing evaluate_newsfeed ", e)

