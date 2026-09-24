import pika

# Подключаемся к RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='order-notifications')

def callback(ch, method, properties, body):
    notification = body.decode('utf-8')
    print(f"[NOTIFICATION] Received: {notification}")

# Говорим RabbitMQ, какую функцию вызывать при получении сообщения
channel.basic_consume(queue='order-notifications', on_message_callback=callback, auto_ack=True)

print("Notification worker started, waiting for messages...")
channel.start_consuming()