import pika
import time
import ssl
from os import environ


hostname = environ.get('rabbitmq_host')
port = environ.get('rabbitmq_port')

# SSL connections are required for Amazon MQ's brokers

if environ.get('stage') == 'production':
    ssl_enabled = True
else:
    ssl_enabled = False

if ssl_enabled:
    username = environ.get('rabbitmq_username')
    password = environ.get('rabbitmq_password')
    credentials = pika.PlainCredentials(username, password)

    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    parameters = pika.ConnectionParameters(host=hostname,
                                           port=port,
                                           virtual_host='/',
                                           credentials=credentials,
                                           ssl_options=pika.SSLOptions(context)
                                           )
else:
    parameters = pika.ConnectionParameters(host=hostname,
                                           port=port)

connected = False
start_time = time.time()

print('Connecting...')

while not connected:
    try:
        connection = pika.BlockingConnection(parameters)
        connected = True
    except pika.exceptions.AMQPConnectionError:
        if time.time() - start_time > 20:
            exit(1)

print('CONNECTED!')

# Create an AMQP topic exchange for Notifications

channel = connection.channel()
exchange_name = "notifications.topic"
exchange_type = "topic"
channel.exchange_declare(exchange=exchange_name,
                         exchange_type=exchange_type, durable=True)

# Create a queue for customer notifications that
# captures ALL order msgs (e.g. order.new, order.cancel)

queue_name = 'Customer_Notifications'
channel.queue_declare(queue=queue_name, durable=True)
channel.queue_bind(exchange=exchange_name,
                   queue=queue_name, routing_key='order.*')

connection.close()
