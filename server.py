"""Script for starting the Highlight Fetcher server."""
import datetime
import json
import logging

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)  # Set the logging level to debug


def confirm_subscription(request_header, request_data):
    """Confirms the SNS subscription."""
    if request_header.get('x-amz-sns-message-type') == 'SubscriptionConfirmation':
        app.logger.info("Got request for confirming subscription")
        app.logger.info(request_header)
        # Extract the request data from the POST body

        subscribe_url = request_data['SubscribeURL']

        # Make an HTTP GET request to the SubscribeURL to confirm the subscription
        # This confirms the subscription with Amazon SNS
        # You can use any HTTP library of your choice (e.g., requests)

        app.logger.info(f"Going to URL: {subscribe_url} to confirm the subscription.")
        response = requests.get(subscribe_url)

        if response.status_code == 200:
            app.logger.info(f"Subscription confirmed. Code: {response.status_code}.")
            return jsonify({'message': 'SubscriptionConfirmed'})
        else:
            app.logger.warning(f"Failed to confirmed subscription. Code {response.status_code}.")
            return jsonify({'message': 'Failed to confirm subscription'}), 500

    return jsonify({"message": "Header does not contain 'x-amz-sns-message-type': 'SubscriptionConfirmation'. No "
                               "subscription to confirm."}), 500


def get_soup(url: str):
    """Returns a BeautifulSoup object for the given URL.

    :arg
        url (str): the URL to fetch.

    :return
        (BeautifulSoup) a BeautifulSoup object of the provided URL.
    """

    page = requests.get(url, headers={'User-agent': 'your bot 0.1'})
    soup = BeautifulSoup(page.text, 'html.parser')
    return soup


def game_clock_to_seconds(period, minutes, seconds):
    return (period - 1) * 12 * 60 + (11 - minutes) * 60 + (60 - seconds)


def seconds_passed(row):
    if len(row['clock'].split(':')) == 2:
        minutes, seconds = (int(x) for x in row['clock'].split(':'))
    elif len(row['clock'].split(':')) == 1:
        minutes = 0
        seconds = int(row['clock'].split('.')[0])

    return game_clock_to_seconds(row['period'], minutes, seconds)


@app.route('/health', methods=["GET"])
def health_check():
    return jsonify({"message": "Health Check OK"}), 200


@app.route('/fetch-highlights', methods=['POST'])
def fetch_highlights():
    request_data = request.data.decode('utf-8')

    # Parse the JSON data into a Python dictionary
    try:
        data = json.loads(request_data)
    except json.JSONDecodeError as e:
        return jsonify({'error': str(e)}), 400

    # if the subscription is confirmed, return after it
    if request.headers.get('x-amz-sns-message-type') == 'SubscriptionConfirmation':
        return confirm_subscription(request.headers, data)

    app.logger.info(f"Extracting request data: {request_data}.")
    data = json.loads(request_data)
    game_id = data['game-id']

    url = f"https://www.espn.com/nba/playbyplay/_/gameId/{game_id}"

    app.logger.info(f"Fetching HTML for Game: {game_id}, from URL: {url}.")
    soup = get_soup(url)

    app.logger.info(f"Parsing HTML for play by plays.")
    # A weird ass script tag that has all the data
    text = soup.find_all('script')[-5].text
    text = text.split('playGrps')[1].split('}]],')[0] + '}]]'
    data = json.loads(text[2:])

    # flatten list
    df = pd.DataFrame([item for sublist in data for item in sublist])

    df['id'] = df['id'].astype(str)
    df['period'] = df['period'].apply(lambda x: x['number'])
    df['text'] = df['text'].fillna('').astype(str)
    df['homeAway'] = df['homeAway'].fillna('neutral').astype('category')
    df['clock'] = df['clock'].apply(lambda x: x['displayValue']).astype(str)
    df['scoringPlay'] = df['scoringPlay'].fillna(False)
    df['secondsPassed'] = df.apply(seconds_passed, axis=1)

    primary_key_name = "game-id"
    sort_key_name = "seconds"

    period_name = "period"
    text_name = "text"
    home_away_name = "venue"
    clock_name = "clock"
    scoring_play_name = "scoring-play"

    app.logger.info(f"Creating {df.shape[0]} items to be sent to Dynamo DB.")
    plays = []
    for id, period, text, home_away, clock, scoring_play, second in zip(df.id, df.period, df.text, df.homeAway, df.clock, df.scoringPlay, df.secondsPassed):
        dynamo_db_item = {
            primary_key_name: id,
            sort_key_name: second,
            period_name: period,
            text_name: text,
            home_away_name: home_away,
            clock_name: clock,
            scoring_play_name: scoring_play
        }
        plays.append(dynamo_db_item)

    table_name = "nba-play-by-play"
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    app.logger.info(f"Sending {len(plays)} items to DynamoDB.")
    num_sent = 0
    with table.batch_writer() as batch:
        for play in plays:
            try:
                response = batch.put_item(Item=play)
                num_sent += 1
            except Exception as e:
                app.logger.warning(f"Could not send item {play} to DynamoDB table {table_name}.", exc_info=e)

    app.logger.info(f"Sent {num_sent} items to DynamoDB.")

    eventbridge_client = boto3.client('events', region_name='eu-north-1')
    event_data = {
        "game-id": game_id,
        "num-plays": num_sent
    }
    app.logger.info(f"Emitting event with data: {event_data}.")
    # PutEvents request to send the custom event

    try:
        response = eventbridge_client.put_events(
            Entries=[
                {
                    'Source': "highlight-fetcher",
                    'DetailType': "PlaysAddedToDynamoDbEvent",
                    'Detail': json.dumps(event_data),
                    'EventBusName': 'default'  # Replace with your EventBridge EventBusName
                }
            ]
        )
        app.logger.info(f"Event successfully emitted.")
    except Exception as e:
        app.logger.warning(f"Could not emit event.", exc_info=e)

    return jsonify({'message': 'Hello from the endpoint'}), 200


@app.route('/hello-world', methods=['GET'])
def hello_world():
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7000)
