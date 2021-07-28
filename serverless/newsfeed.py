# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import requests
from bs4 import BeautifulSoup
import json
import boto3
import json
from datetime import datetime
import os
import common
import match
import watchlist
import logging
log = logging.getLogger()
log.setLevel(logging.INFO)
stop_words = ['One', 'morning', ',', 'when', 'Gregor', 'Samsa', 'woke', 'from', 'troubled', 'dreams', ',', 'he', 'found', 'himself', 'transformed', 'in', 'his', 'bed', 'into', 'a', 'horrible', 'vermin', '.', 'He', 'lay', 'on', 'his', 'armour-like', 'back', ',', 'and', 'if', 'he', 'lifted', 'his', 'head', 'a', 'little', 'he', 'could', 'see', 'his', 'brown', 'belly', ',', 'slightly', 'domed', 'and', 'divided', 'by', 'arches', 'into', 'stiff', 'sections', '.', 'The', 'bedding', 'was', 'hardly', 'able', 'to', 'cover', 'it', 'and', 'seemed', 'ready', 'to', 'slide', 'off', 'any', 'moment', '.', 'His', 'many', 'legs', ',', 'pitifully', 'thin', 'compared', 'with', 'the', 'size', 'of', 'the', 'rest', 'of', 'him', ',', 'waved', 'about', 'helplessly', 'as', 'he', 'looked', '.', '``', 'What', "'s", 'happened', 'to']


def query_newsfeed(event, context):
    """
    Submit a request with a newsfeed information to be processed
    Web service call format:
    {
    "url":"https://comicbook.com/starwars/news/lego-star-wars-probe-droid-darth-vader-scout-trooper-helmet-order/",
    "html_tag":"article",
    "html_attribute":{
        "itemprop": "articleBody"
    },
    "options": {
        "extract_entities": "true",
        "extract_keyphrase": "true",
        "extract_sentiment": "true"
    }
    }
    :param event: see web service example
    :param context:
    :return: the message ID for the submitted job
    """
    log.info("Hello From query_newsfeed")

    # Default options:

    extract_entities = False
    extract_keyphrase = False
    extract_sentiment = False
    default_options = {
        "extract_entities": "true",
        "extract_keyphrase": "false",
        "extract_sentiment": "true"
    }
    newsfeed_name_ts = "news_" + datetime.now().strftime("%Y-%d-%mT%H:%M:%S")

    try:
        req_body = json.loads(event['body'])
        # print(req_body)
        url = req_body.get('url')
        html_tag = req_body.get('html_tag')
        html_attribute = req_body.get('html_attribute')
        newsfeed_name = req_body.get('newsfeed_name', newsfeed_name_ts)
        options = req_body.get('options', default_options)

        if options["extract_entities"] == "true":
            extract_entities = True
        if options["extract_keyphrase"] == "true":
            extract_keyphrase = True
        if options["extract_sentiment"] == "true":
            extract_sentiment = True
        log.info("newsfeed_name " + newsfeed_name)

        # get secret configuration
        config = common.get_secret(os.environ['SECRET'])
        newsfeed_bucket = config['newsfeed-bucket']
        queue_name = config['incoming-newsfeed-queue']

        # scrap webpage
        scraped_text = scrape_webpage(url, html_tag, html_attribute)
        # save page text to bucket
        common.save_content_to_bucket(newsfeed_bucket, "newsfeed", newsfeed_name, ".txt", scraped_text)
        # push message to queue
        sqs_response = push_message_to_queue(queue_name, newsfeed_bucket, newsfeed_name, scraped_text, url,
                                             extract_entities, extract_keyphrase, extract_sentiment)
    except Exception as e:
        log.error("Error executing query_newsfeed ", e)
        return {
                'statusCode': 500,
                'body': json.dumps({
                    "error": str(e)
                })
            }

    response = {
        "options": {
            "extract_entities": extract_entities,
            "extract_keyphrase": extract_keyphrase,
            "extract_sentiment": extract_sentiment
        },
        "file_generated": newsfeed_name + ".txt",
        "bucket_used": newsfeed_bucket,
        "url": url,
        "Message ID": str(sqs_response.get('MessageId', "Error"))
    }

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }


def push_message_to_queue(queue_name, newsfeed_bucket, newsfeed_name, scraped_text, url,
                          extract_entities, extract_keyphrase, extract_sentiment):
    """
    Push a message to the newsfeed queue and return the message id from SQS Service
    :param queue_name: the queue name to push the message
    :param newsfeed_bucket: the bucket name of the related message
    :param newsfeed_name: the name of the newsfeed
    :param scraped_text: the scraped clean content
    :param url: the url of the resource
    :param extract_entities: True/False
    :param extract_keyphrase: True/False
    :param extract_sentiment: True/False
    :return: the sqs response including the message id
    """
    sqs = boto3.resource('sqs')
    item = {
        "bucket": newsfeed_bucket,
        "file": newsfeed_name + ".txt",
        "content": scraped_text,
        "url": url,
        "options": {
            "extract_entities": extract_entities,
            "extract_keyphrase": extract_keyphrase,
            "extract_sentiment": extract_sentiment,
        }
    }

    queue = sqs.get_queue_by_name(QueueName=queue_name)
    log.info("Processing {0} to SQS".format(newsfeed_name))
    sqs_response = queue.send_message(MessageBody=json.dumps(item))
    return sqs_response


def evaluate_newsfeed(event, context):
    """
    Queue Message Handler which process a newsfeed message
    :param event:
    :param context:
    :return: Call Match-logic with the resulted Match
    """
    log.info("Hello From process_newsfeed")
    config = common.get_secret(os.environ['SECRET'])
    sentiment_result = ""
    entities_result = ""
    keyphrase_result = ""

    # Iterate on messages available in the Queue
    for message in event['Records']:
        message_id = message["messageId"]
        message_body = json.loads(message["body"])
        message_file = message_body["file"]
        message_content = message_body["content"]
        message_options = message_body["options"]
        log.info("Processing file {0} with Message ID {1}.".format(message_file, message_id))
        url = message_body['url']
        try:
            client = boto3.client('comprehend')
            newsfeed_bucket = config['newsfeed-bucket']
            if message_options["extract_entities"]:
                log.info("extracting entities")
                entities_result = extract_comprehend_entities(client, message_content)
                common.save_content_to_bucket(newsfeed_bucket, "entities", message_id, ".json", entities_result, "JSON")
            if message_options["extract_keyphrase"]:
                log.info("extracting keyphrase")
                keyphrase_result = extract_comprehend_keyphrase(client, message_content)
                common.save_content_to_bucket(newsfeed_bucket, "keyphrases", message_id, ".json", keyphrase_result, "JSON")
            if message_options["extract_sentiment"]:
                log.info("extracting sentiment")
                sentiment_result = extract_comprehend_sentiment(client, message_content)
                common.save_content_to_bucket(newsfeed_bucket, "sentiments", message_id, ".json", sentiment_result, "JSON")

            results = query_message_match_result(entities_result, keyphrase_result)

            # log.info(results)
            if len(results) > 0:
                log.info("Match Found!")
                content = {
                    "results": results,
                    "url": url,
                    "sentiment": sentiment_result
                }
                # calling match logic with provided match result
                match.detect_watchlist(content)

        except Exception as e:
            log.error("Error executing process_newsfeed with Message", exc_info=True)
    return "Processed {0} records.".format(len(event['Records']))


def query_message_match_result(entities_result, keyphrase_result):
    """
    Build and execute a query list for a given message and return the match result
    :param entities_result: comprehend entities list
    :param keyphrase_result: comprehend keyphrase list
    :return: all passed match results
    """
    results = []
    query_list = []
    log.info("Processing Entities")
    entities_clean = clean_words(entities_result['Entities'])
    query_list.extend(entities_clean)
    log.info("Processing KeyPhrases")
    keyphrase_clean = clean_words(keyphrase_result['KeyPhrases'])
    query_list.extend(keyphrase_clean)
    query_no_duplicates = list(dict.fromkeys(query_list))
    log.info("Collected {0} queries".format(len(query_no_duplicates)))

    # Iterate on all query keys
    for key in query_no_duplicates:
        query, params = watchlist.get_keyword_query(key)
        query_result = watchlist.execute_statement(query, params)
        if len(query_result['records']) > 0:
            log.info(query_result['records'])
            for rec in query_result['records']:
                results.append({
                    'entity': rec[0]['stringValue'],
                    'entity_type': rec[1]['stringValue'],
                    'create_timestamp': rec[2]['stringValue']
                })
    log.info("Matched {0}".format(len(results)))
    return results


def clean_words(content):
    query_list = []
    for entity in content:
        if entity['Score'] >= 0.9:
            keyword = entity['Text']
            words_lowered = [w.lower() for w in keyword.split()]
            words = [w for w in words_lowered if not w in stop_words]
            query_list.extend(words)
    log.info(len(query_list))
    return query_list


def scrape_webpage(url, html_tag, html_attribute):
    """
    Scrap a web page using BeautifulSoup Library and html qualifier
    :param url: url of the newsfeed
    :param html_tag: html qualifier indicating the section of the news
    :param html_attribute: the matching html attribute to select a particular html tag
    :return: the scraped text of the html page
    """
    log.info("Scraping : {0}".format(url))
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    article = soup.find(html_tag, html_attribute)
    results = article.text
    log.info("Finished Scraping : {0}".format(url))
    return results


def extract_comprehend_entities(client, input_text):
    """
    Calling Comprehend Entities API
    :param client: boto3 comprehend client instance
    :param input_text: the input text
    :return: Comprehend response
    """
    # For more info on limited_text check - https://docs.aws.amazon.com/comprehend/latest/dg/API_DetectEntities.html TextSizeLimitExceededException
    response_entities = client.detect_entities(
        Text=common.limited_text(input_text, 5000),
        LanguageCode='en'
    )
    return response_entities


def extract_comprehend_keyphrase(client, input_text):
    """
    Calling Comprehend KeyPhrase API
    :param client: boto3 comprehend client instance
    :param input_text: the input text
    :return: Comprehend response
    """
    response_key_phrases = client.detect_key_phrases(
        Text=common.limited_text(input_text, 5000),
        LanguageCode='en'
    )
    return response_key_phrases


def extract_comprehend_sentiment(client, input_text):
    """
    Calling Comprehend Sentiment API
    :param client: boto3 comprehend client instance
    :param input_text: the input text
    :return: Comprehend response
    """
    response_sentiment = client.detect_sentiment(
        Text=common.limited_text(input_text, 5000),
        LanguageCode='en'
    )
    return response_sentiment
