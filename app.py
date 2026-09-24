import os
import json
import mysql.connector
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import pika

app = Flask(__name__)

MYSQL_HOST = 'mysql'
MYSQL_USER = 'root'
MYSQL_PASSWORD = os.getenv('MYSQL_ROOT_PASSWORD', 'root')
MYSQL_DB = os.getenv('MYSQL_DATABASE', 'orders_db')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
RABBITMQ_HOST = 'rabbitmq'

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB
    )

@app.route('/api/orders', methods=['POST'])
def create_order():
    data = request.json
    buyer, product, amount = data.get('buyer'), data.get('product'), data.get('amount')

    # 1. Сохраняем в MySQL
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO orders (buyer, product, amount) VALUES (%s, %s, %s)",
        (buyer, product, amount)
    )
    conn.commit()
    order_id = cursor.lastrowid
    cursor.close()
    conn.close()

    # 2. Отправляем в Kafka (ключ = order_id)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    event = {'order_id': order_id, 'buyer': buyer, 'product': product, 'amount': float(amount)}
    producer.send('order-events', key=order_id, value=event)
    producer.flush()
    producer.close()

    # 3. Отправляем в RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='order-notifications')
    notification = f"New order {order_id} for {buyer}: {product} (${amount})"
    channel.basic_publish(exchange='', routing_key='order-notifications', body=notification)
    connection.close()

    return jsonify({'status': 'success', 'order_id': order_id}), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)