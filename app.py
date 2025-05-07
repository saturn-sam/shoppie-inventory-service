import os
import json
import pika
import jwt
from datetime import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, Unauthorized, NotFound

app = Flask(__name__)
CORS(app)

import logging
import sys
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, self.datefmt),
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
handler.setLevel(logging.INFO)

app.logger.handlers = [handler]
app.logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('/var/log/inventory.log')
file_handler.setFormatter(JsonFormatter())
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)

# Database configuration
# app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5433/admin_db')
uri = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5433/admin_db')
if uri.startswith('postgres://'):
    uri = uri.replace('postgres://', 'postgresql://', 1)

app.logger.info(f"Database URI: {uri}")
app.config['SQLALCHEMY_DATABASE_URI'] = uri

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)

# RabbitMQ configuration
RABBITMQ_URL = os.environ.get('MESSAGE_QUEUE_URL', 'amqp://guest:guest@localhost:5672')
EXCHANGE_NAME = 'product_events'
app.logger.info(f"RabbitMQ URL: {RABBITMQ_URL} Exchange Name: {EXCHANGE_NAME}")
# JWT configuration
JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'your-secret-key')


# Models
class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text, nullable=False)
    price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=0)
    image = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'price': self.price,
            'quantity': self.quantity,
            'image': self.image,
            'createdAt': self.created_at.isoformat(),
            'updatedAt': self.updated_at.isoformat()
        }

# Message queue functions
def publish_message(routing_key, message):
    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)
        
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        app.logger.info(f"Message published.")
        connection.close()
        return True
    except Exception as e:
        app.logger.error(f"Error publishing message: {str(e)}")
        return False

# Authentication middleware
def token_required(f):
    def decorator(*args, **kwargs):
        token = None
        auth_header = request.headers.get('Authorization')
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
        
        if not token:
            app.logger.warning('Token is missing')
            raise Unauthorized('Token is missing')
        
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])
            if not payload.get('user_id'):
                app.logger.warning('Invalid token payload')
                raise Unauthorized('Invalid token')
            
            # Check if user is staff
            if not payload.get('is_staff', False):
                app.logger.warning('User is not staff')
                raise Unauthorized('Admin access required')
                
        except jwt.ExpiredSignatureError:
            app.logger.warning('Token has expired')
            raise Unauthorized('Token has expired')
        except jwt.InvalidTokenError:
            app.logger.warning('Invalid token')
            raise Unauthorized('Invalid token')
        
        return f(*args, **kwargs)
    
    decorator.__name__ = f.__name__
    return decorator

# Routes
@app.route('/inventory-api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'inventory-service'}), 200

@app.route('/inventory-api/products', methods=['GET'])
@token_required
def get_products():
    products = Product.query.all()
    app.logger.info(f"Retrieved {len(products)} products")
    return jsonify([product.to_dict() for product in products])

@app.route('/inventory-api/products/<int:product_id>', methods=['GET'])
@token_required
def get_product(product_id):
    product = Product.query.get_or_404(product_id)
    app.logger.info(f"Retrieved product {product_id}")
    return jsonify(product.to_dict())

@app.route('/inventory-api/products/inter-svc/<int:product_id>', methods=['GET'])
def get_product_inter_svc(product_id):
    product = Product.query.get_or_404(product_id)
    app.logger.info(f"Retrieved product {product_id} for inter-service call")
    return jsonify(product.to_dict())

@app.route('/inventory-api/products', methods=['POST'])
@token_required
def create_product():
    data = request.get_json()
    
    required_fields = ['name', 'description', 'price', 'quantity']
    for field in required_fields:
        if field not in data:
            app.logger.warning(f"Missing required field: {field}")
            raise BadRequest(f"Missing required field: {field}")
    
    product = Product(
        name=data['name'],
        description=data['description'],
        price=data['price'],
        quantity=data['quantity'],
        image=data.get('image', '')
    )
    
    db.session.add(product)
    db.session.commit()
    
    # Publish message to queue
    app.logger.info(f"Created product {product.id}")
    publish_message('product.created', product.to_dict())
    app.logger.info(f"Published product.created message for product {product.id}")
    
    return jsonify(product.to_dict()), 201

@app.route('/inventory-api/products/<int:product_id>', methods=['PUT'])
@token_required
def update_product(product_id):
    product = Product.query.get_or_404(product_id)
    app.logger.info(f"Updating product {product_id}")
    data = request.get_json()
    
    if 'name' in data:
        product.name = data['name']
    if 'description' in data:
        product.description = data['description']
    if 'price' in data:
        product.price = data['price']
    if 'quantity' in data:
        product.quantity = data['quantity']
    if 'image' in data:
        product.image = data['image']
    
    db.session.commit()
    app.logger.info(f"Updated product {product_id}")
    # Publish message to queue
    publish_message('product.updated', product.to_dict())
    app.logger.info(f"Published product.updated message for product {product.id}")
    
    return jsonify(product.to_dict())

@app.route('/inventory-api/products/<int:product_id>', methods=['DELETE'])
@token_required
def delete_product(product_id):
    product = Product.query.get_or_404(product_id)
    app.logger.info(f"Deleting product {product_id}")
    # Store product data before deletion for queue message
    product_data = product.to_dict()
    
    db.session.delete(product)
    db.session.commit()
    app.logger.info(f"Deleted product {product_id}")
    # Publish message to queue
    publish_message('product.deleted', product_data)
    app.logger.info(f"Published product.deleted message for product {product.id}")
    return '', 204

# Error handlers
@app.errorhandler(BadRequest)
def handle_bad_request(e):
    app.logger.error(f"Bad request: {str(e)}")
    return jsonify({'error': str(e)}), 400

@app.errorhandler(Unauthorized)
def handle_unauthorized(e):
    app.logger.error(f"Unauthorized: {str(e)}")
    return jsonify({'error': str(e)}), 401

@app.errorhandler(NotFound)
def handle_not_found(e):
    app.logger.error(f"Not found: {str(e)}")
    return jsonify({'error': str(e)}), 404

@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f"Unhandled exception: {str(e)}")
    return jsonify({'error': 'Internal server error'}), 500

import threading
# from consumer import consume_purchase_events

from consumer import consume_purchase_events

app.logger.info("Starting consumer thread...")
threading.Thread(target=consume_purchase_events, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


