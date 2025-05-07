import os
import json
import pika
import time
from flask import Flask
from app import app, db, Product, RABBITMQ_URL, EXCHANGE_NAME

def consume_purchase_events():
    """
    Consume purchase events from the client service.
    Updates product quantities when purchases are made.
    """
    app.logger.info("Starting purchase events consumer...")

    # Connection retry logic
    connection = None
    retries = 5
    delay = 5  # seconds

    for attempt in range(retries):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            break
        except Exception as e:
            app.logger.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
            if attempt < retries - 1:
                app.logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                app.logger.error("Failed to connect to RabbitMQ after several attempts")
                return

    if not connection:
        return

    channel = connection.channel()

    # Declare exchange
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

    # Declare queue
    result = channel.queue_declare(queue='admin_service_queue', durable=True)
    queue_name = result.method.queue

    # Bind queue to routing key
    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=queue_name,
        routing_key='purchase.created'
    )

    def callback(ch, method, properties, body):
        try:
            with app.app_context():
                data = json.loads(body)
                app.logger.info(f"Received purchase event: {data}")

                items = data.get('data', [])
                if not isinstance(items, list):
                    app.logger.error("Invalid data format in purchase event")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                for item in items:
                    product_id = item.get('productId')
                    quantity = item.get('quantity', 1)

                    if not product_id or quantity <= 0:
                        app.logger.warning(f"Skipping invalid item: {item}")
                        continue

                    product = Product.query.get(product_id)
                    if product:
                        if product.quantity >= quantity:
                            product.quantity -= quantity
                            app.logger.info(f"Updated product {product_id}: -{quantity} => {product.quantity}")
                        else:
                            app.logger.warning(f"Not enough quantity for product {product_id}: requested {quantity}, available {product.quantity}")
                    else:
                        app.logger.error(f"Product ID {product_id} not found")

                db.session.commit()

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            app.logger.error(f"Error processing purchase event: {str(e)}")
            db.session.rollback()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    app.logger.info("Started consuming 'purchase.created' events")
    channel.start_consuming()

if __name__ == '__main__':
    consume_purchase_events()
