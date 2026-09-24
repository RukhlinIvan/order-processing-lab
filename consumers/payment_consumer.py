import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'order-events',
    bootstrap_servers='kafka:9092',
    group_id='payment-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Payment consumer started, waiting for events...")
for message in consumer:
    order = message.value
    print(f"[PAYMENT] Processing payment for order {order.get('order_id')}, amount: {order.get('amount')}")