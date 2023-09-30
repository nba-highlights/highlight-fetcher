"""Script for starting the Highlight Fetcher server."""
import json
import logging
from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)  # Set the logging level to debug


# the executor pool used by the splitting endpoint
executor = ThreadPoolExecutor(2)

# the futures store. If a game is currently being processed, it will be stored here in the meantime.
futures = {}

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
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                      'Mobile/15E148'}

    page = requests.get(url, headers=headers)
    app.logger.info(f"Status code: {page.status_code}")
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


@app.route('/fetch-highlights', methods=['POST', 'GET'])
def fetch_highlights():

    if request.method == 'GET':
        game_id = request.args.get("game-id")
    elif request.method == 'POST':
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
    else:
        return jsonify({'message': f'Method {request.method} not allowed.'}), 400

    """
    if game_id in futures:
        if not futures[game_id].done():
            app.logger.info(f"The game {game_id} is already being processed.")
            return jsonify({"message": "Game is already being processed."}), 200
        else:
            app.logger.info(f"The game {game_id} finished processing.")
            del futures[game_id]
    """
    app.logger.info(f"Starting process for fetching Game: {game_id}.")

    return _fetch_highlights(game_id).to_json(), 200
    #future = executor.submit(_fetch_highlights, game_id)
    #futures[game_id] = future

    #return jsonify({'message': f'Fetched highlights for game: {game_id}'}), 200


def _fetch_highlights(game_id):
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
    sort_key_name = "id"
    period_name = "period"
    text_name = "text"
    home_away_name = "venue"
    clock_name = "clock"
    seconds_name = "seconds"
    scoring_play_name = "scoring-play"
    return df
    app.logger.info(f"Creating {df.shape[0]} items to be sent to Dynamo DB.")
    plays = []
    for id, period, text, home_away, clock, scoring_play, second in zip(df.id, df.period, df.text, df.homeAway,
                                                                        df.clock, df.scoringPlay, df.secondsPassed):
        dynamo_db_item = {
            primary_key_name: game_id,
            sort_key_name: id,
            period_name: period,
            text_name: text,
            seconds_name: second,
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
                batch.put_item(Item=play)
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
        app.logger.info(f"Event successfully emitted. {response}")
    except Exception as e:
        app.logger.warning(f"Could not emit event.", exc_info=e)


@app.route('/hello-world', methods=['GET'])
def hello_world():
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7000)
