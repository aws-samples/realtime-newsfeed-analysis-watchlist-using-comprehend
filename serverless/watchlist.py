# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import common
from datetime import datetime
import os
import boto3
import pandas as pd
import json
import logging
log = logging.getLogger()
log.setLevel(logging.INFO)

# Global variable for RDS Connection
rds_client = None
config = None


def check_keyword(event, context):
    """
    Check an a keyword/keywords against the watchlist using Fuzzy name matching
    Message example
    {
    "keywords": ["Tauntaon", "Luke Skiwalker"]
    }
    :param event: see the message example
    :param context:
    :return: the result of the match
    """
    try:
        req_body = json.loads(event['body'])
        keywords = req_body["keywords"]
        results = []
        for keyword in keywords:
            statement, parameters = get_keyword_query(keyword)
            query_result = execute_statement(statement, parameters)

            results.append(query_result)
    except Exception as e:
        log.error("Error executing check_keyword ", e)
        return {
            'statusCode': 500,
            'body': json.dumps({
                "error": "Something went wrong executing check_keyword, please check the logs"
            })
        }

    response = {
        "results": results
    }

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }


def refresh(event, context):
    """
    This method refresh the watchlist data in the DB, by cleaning the table and reloading new data
    {
    "refresh_list_from_bucket": false,
    "watchlist": [
        {"entity":"Luke Skywalker", "entity_type": "person"},
        {"entity":"Jedi Global Financial", "entity_type": "organization"},
        {"entity":"Droid", "entity_type": "product"}
    ]
    }
    :param event: see message example
    :param context:
    :return:
    """
    wl_timestamp = "WL_" + datetime.utcnow().strftime("%Y-%d-%mT%H:%M:%S")
    log.info("timestamp for message refresh " + wl_timestamp)
    try:
        req_body = json.loads(event['body'])
        config = common.get_secret(os.environ['SECRET'])
        if 'refresh_list_from_bucket' in req_body:
            refresh_list_from_bucket = req_body.get('refresh_list_from_bucket')
        if 'watchlist' in req_body:
            watchlist = req_body.get('watchlist')
        log.info(refresh_list_from_bucket)
        log.info(watchlist)
        if refresh_list_from_bucket:
            newsfeed_bucket = config['newsfeed-bucket']
            input_file = "watchlist/watchlist.csv"
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=newsfeed_bucket, Key=input_file)
            csv_data = pd.read_csv(obj['Body'])
            csv_watchlist = []
            for i, row in csv_data.iterrows():
                record = {'entity': row[0], 'entity_type': row[1]}
                csv_watchlist.append(record)
            recreate_db()
            insert_records(csv_watchlist)
        else:
            recreate_db()
            insert_records(watchlist)
        response = execute_statement('select count(*) from WatchList')
        result = response
    except Exception as e:
        log.error("Error executing refresh_watchlist ", e)
        return {
                'statusCode': 500,
                'body': json.dumps({
                    "error": "Error executing refresh watchlist , please check the logs"
                })
            }

    response = {
        "refresh_list_from_bucket": refresh_list_from_bucket,
        "refresh_list_timestamp": wl_timestamp,
        "result": result
    }

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }


def recreate_db():
    """
    This method delete the watchlist data
    :return:
    """
    execute_statement(get_watchlist_table_sql())
    execute_statement("truncate table watchlist")
    execute_statement('create extension IF NOT EXISTS fuzzystrmatch;')


def execute_statement(sql, sql_parameters=[]):
    """
    Execute a sql statement against an Aurora serverless Data API
    sql_parameters as a means to prevent SQL injections
    :param sql: the SQL Statement
    :param sql_parameters: sql statement params - if exists
    :return: the result of the query execution
    """

    client = get_rds_connection()
    if sql_parameters:
        response = client.execute_statement(
            secretArn=config['db-secret'],
            database='postgres',
            resourceArn=config['db-cluster-arn'],
            sql=sql,
            parameters=sql_parameters
        )
    else:
        response = client.execute_statement(
            secretArn=config['db-secret'],
            database='postgres',
            resourceArn=config['db-cluster-arn'],
            sql=sql
        )
    return response


def get_watchlist_table_sql():
    """
    Returns the watchlist DDL table creation SQL Statement
    :return:
    """
    query = 'CREATE TABLE IF NOT EXISTS WatchList( ' \
            'entity varchar(255), ' \
            'entity_type varchar(255), ' \
            'create_datetime timestamp, ' \
            'PRIMARY KEY (entity, entity_type)) '
    return query


def insert_records(watchlist):
    """
    Iterate on the watchlist values and create and insert records to the watchlist table
    :param watchlist: array of entity and entity_type values
    :return:
    """

    for record in watchlist:
        sql_parameters = [
                            {'name': 'entity', 'value': {'stringValue': "{0}".format(record["entity"])}},
                            {'name': 'entity_type', 'value': {'stringValue': "{0}".format(record["entity_type"])}}
                          ]
        statement = "INSERT INTO watchlist(entity, entity_type, create_datetime) VALUES(:entity, :entity_type, timezone('UTC', now()))"
        execute_statement(statement, sql_parameters)
    execute_statement('commit;')


def get_keyword_query(keyword):
    """
    Generate the corresponding SQL Statement and SQL parameters for query the watchlist DB
    :param keyword:
    :return: the statement and statement parameters
    """
    sql_parameters = [{'name': 'input_keyword', 'value': {'stringValue': "{0}".format(keyword)}}]
    statement = "SELECT * FROM  watchlist WHERE soundex(lower(entity)) = soundex(lower(:input_keyword)) " \
                "union " \
                "SELECT * FROM  watchlist WHERE levenshtein_less_equal(lower(entity), lower(:input_keyword),2) <=2"
    return statement, sql_parameters


def get_rds_connection():
    global rds_client
    global config
    if rds_client is None:
        rds_client = boto3.client('rds-data')
    if config is None:
        config = common.get_secret(os.environ['SECRET'])
    return rds_client
