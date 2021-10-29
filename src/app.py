import os
import json
import requests
import amqp_setup
import pika
import datetime

from flask import Flask, request, jsonify
from flask_cors import CORS
from prometheus_flask_exporter import PrometheusMetrics

if os.environ.get('stage') == 'production':
    games_service_url = os.environ.get('games_service_url')
    orders_service_url = os.environ.get('orders_service_url')
else:
    games_service_url = os.environ.get('games_service_url_internal')
    orders_service_url = os.environ.get('orders_service_url_internal')

app = Flask(__name__)

metrics = PrometheusMetrics(app)

CORS(app)


@metrics.do_not_track()
@app.route('/health')
def health_check():
    return jsonify(
        {
            'message': 'Place order service is healthy!',
            'time': str(datetime.datetime.now())
        }
    ), 200


@app.route('/place-order', methods=['POST'])
def place_order():
    data = request.get_json()

    # (1) Reserve the games
    cart_items = data['cart_items']
    for item in cart_items:
        item_response = requests.patch(
            games_service_url + '/games/' + str(item['game_id']),
            data=json.dumps({
                'reserve': item['quantity']
            }),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If an item was not successfully reserved (status code != 200),
        # revert all previous item reservations
        if item_response.status_code != 200:
            revert_cart_reservations(cart_items, cart_items.index(item))
            return jsonify(
                {
                    'message': 'Unable to place order.',
                    'error': 'Unable to reserve required game stock.'
                }
            ), 500

    # (2) Create the order
    order_response = requests.post(
        orders_service_url + '/orders',
        data=json.dumps({
            'customer_email': data['customer_email'],
            'cart_items': data['cart_items']
        }),
        headers={
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    )

    # If order record was not created successfully, revert all
    # items previously reserved
    if order_response.status_code != 201:
        revert_cart_reservations(cart_items, len(cart_items))
        return jsonify(
            {
                'message': 'Unable to place order.',
                'error': 'Unable to create order record.'
            }
        ), 500

    # (3) Send notification to the AMQP broker
    notification_data = {
        'email': data['customer_email'],
        'data': data['cart_items']
    }

    connection = pika.BlockingConnection(amqp_setup.parameters)

    channel = connection.channel()

    channel.basic_publish(
        exchange=amqp_setup.exchange_name, routing_key='order.new',
        body=json.dumps(notification_data),
        properties=pika.BasicProperties(delivery_mode=2))

    connection.close()

    return jsonify(
        {
            'message': 'Order placed.',
            'data': order_response.json()['data']
        }
    ), 200


def revert_cart_reservations(cart_items, idx):
    '''Reverts all cart reservations up to index `idx` exclusive'''
    for i in range(idx):
        item = cart_items[i]
        requests.patch(
            games_service_url + '/games/' + str(item['game_id']),
            data=json.dumps({
                'reserve': -item['quantity']
            }),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
