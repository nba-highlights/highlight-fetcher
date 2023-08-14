"""Script for starting the Highlight Fetcher server."""

import json
import logging
import os
from io import BytesIO

import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By

from pathlib import Path

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


@app.route('/health', methods=["GET"])
def health_check():
    return jsonify({"message": "Health Check OK"}), 200


@app.route('/fetch-highlights', methods=['GET'])
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

    game_id = request.args.get('game-id')

    url = f"https://www.espn.com/nba/playbyplay/_/gameId/{game_id}"

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument("window-size=1920,1080")
    driver = webdriver.Chrome(options)
    driver.get(url)

    buttons = driver.find_elements(By.TAG_NAME, "button")

    # Click Continue Without Accepting button to make visible the quarters
    for button in buttons:
        if button.accessible_name == "Continue without Accepting":
            button.click()
            break

    buttons = driver.find_elements(By.TAG_NAME, "button")
    quarter_buttons = list(filter(lambda button: button.text in ["1st", "2nd", "3rd", "4th"], buttons))

    for quarter_button in quarter_buttons:
        quarter_button.click()

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        soup.find_all()

    return jsonify({'message': 'Hello from the endpoint'}), 200


@app.route('/hello-world', methods=['GET'])
def hello_world():
    return "Hello World"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
