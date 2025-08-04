# File: api/app.py
#
# Flask RESTful API for customer data and order management.
# Prerequisites:
#   1. SQLite database created at ../database/ecommerce.db
#   2. pip install Flask Flask-CORS
#
# Endpoints:
#   GET /api/health
#   GET /api/customers?page=<int>&per_page=<int>
#   GET /api/customers/<int:customer_id>
#   GET /api/customers/<int:customer_id>/orders
#   GET /api/orders?page=<int>&per_page=<int>&status=<string>
#   GET /api/orders/<int:order_id>
#
# Run with:  python app.py
# --------------------------------------------------------------

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '..', 'database', 'ecommerce.db'))


# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------
def get_db():
    """Return SQLite connection with rows as dict‐like objects."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row):
    """Convert sqlite3.Row to regular dict."""
    return {k: row[k] for k in row.keys()}


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple health-check endpoint."""
    try:
        conn = get_db()
        total = conn.execute('SELECT COUNT(*) AS c FROM users').fetchone()['c']
        conn.close()
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'total_customers': total,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }), 200
    except Exception as exc:
        return jsonify({'status': 'unhealthy', 'error': str(exc)}), 500


@app.route('/api/customers', methods=['GET'])
def list_customers():
    """List customers with pagination and order count."""
    page = max(request.args.get('page', default=1, type=int), 1)
    per_page = min(request.args.get('per_page', default=20, type=int), 100)
    offset = (page - 1) * per_page

    conn = get_db()
    total = conn.execute('SELECT COUNT(*) AS c FROM users').fetchone()['c']

    customers = conn.execute('''
        SELECT  u.id,
                u.first_name,
                u.last_name,
                u.email,
                u.age,
                u.gender,
                u.country,
                u.city,
                u.created_at,
                COUNT(o.order_id)      AS order_count,
                COALESCE(SUM(o.num_of_item), 0) AS total_items
        FROM users AS u
        LEFT JOIN orders AS o ON o.user_id = u.id
        GROUP BY u.id
        ORDER BY u.id
        LIMIT ? OFFSET ?
    ''', (per_page, offset)).fetchall()
    conn.close()

    return jsonify({
        'customers': [row_to_dict(r) for r in customers],
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total_records': total,
            'total_pages': (total + per_page - 1) // per_page,
            'has_next': page * per_page < total,
            'has_prev': page > 1
        }
    }), 200


@app.route('/api/customers/<int:customer_id>', methods=['GET'])
def get_customer(customer_id):
    """Return specific customer details + order statistics."""
    conn = get_db()
    customer = conn.execute('''
        SELECT  u.*,
                COUNT(o.order_id)          AS total_orders,
                COALESCE(SUM(o.num_of_item), 0) AS total_items
        FROM users AS u
        LEFT JOIN orders AS o ON o.user_id = u.id
        WHERE u.id = ?
        GROUP BY u.id
    ''', (customer_id,)).fetchone()

    if customer is None:
        conn.close()
        return jsonify({'error': 'Customer not found'}), 404

    # order status breakdown
    status_rows = conn.execute('''
        SELECT status, COUNT(*) AS count
        FROM orders
        WHERE user_id = ?
        GROUP BY status
    ''', (customer_id,)).fetchall()

    # most recent 10 orders
    recent_orders = conn.execute('''
        SELECT order_id, status, created_at, num_of_item,
               shipped_at, delivered_at
        FROM orders
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT 10
    ''', (customer_id,)).fetchall()
    conn.close()

    data = row_to_dict(customer)
    data['order_status_breakdown'] = [row_to_dict(r) for r in status_rows]
    data['recent_orders'] = [row_to_dict(r) for r in recent_orders]

    return jsonify({'customer': data}), 200


@app.route('/api/customers/<int:customer_id>/orders', methods=['GET'])
def get_customer_orders(customer_id):
    """Get all orders for a specific customer with pagination."""
    try:
        # Pagination parameters
        page = max(request.args.get('page', default=1, type=int), 1)
        per_page = min(request.args.get('per_page', default=20, type=int), 100)
        offset = (page - 1) * per_page
        
        conn = get_db()
        
        # Check if customer exists
        customer = conn.execute('SELECT id FROM users WHERE id = ?', (customer_id,)).fetchone()
        if not customer:
            conn.close()
            return jsonify({'error': 'Customer not found'}), 404
        
        # Get total orders count for this customer
        total_orders = conn.execute(
            'SELECT COUNT(*) AS c FROM orders WHERE user_id = ?', 
            (customer_id,)
        ).fetchone()['c']
        
        # Get orders for this customer with pagination
        orders = conn.execute('''
            SELECT  o.order_id,
                    o.status,
                    o.created_at,
                    o.shipped_at,
                    o.delivered_at,
                    o.returned_at,
                    o.num_of_item,
                    u.first_name,
                    u.last_name,
                    u.email
            FROM orders AS o
            JOIN users AS u ON o.user_id = u.id
            WHERE o.user_id = ?
            ORDER BY o.created_at DESC
            LIMIT ? OFFSET ?
        ''', (customer_id, per_page, offset)).fetchall()
        
        conn.close()
        
        # Calculate pagination info
        total_pages = (total_orders + per_page - 1) // per_page
        
        return jsonify({
            'customer_id': customer_id,
            'orders': [row_to_dict(order) for order in orders],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_orders': total_orders,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/api/orders/<int:order_id>', methods=['GET'])
def get_order_details(order_id):
    """Get specific order details with customer information."""
    try:
        conn = get_db()
        
        # Get order details with customer information
        order = conn.execute('''
            SELECT  o.order_id,
                    o.user_id,
                    o.status,
                    o.created_at,
                    o.shipped_at,
                    o.delivered_at,
                    o.returned_at,
                    o.num_of_item,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.city,
                    u.state,
                    u.country,
                    u.street_address,
                    u.postal_code
            FROM orders AS o
            JOIN users AS u ON o.user_id = u.id
            WHERE o.order_id = ?
        ''', (order_id,)).fetchone()
        
        conn.close()
        
        if not order:
            return jsonify({'error': 'Order not found'}), 404
        
        # Convert to dictionary and structure response
        order_data = row_to_dict(order)
        
        # Organize data into logical sections
        response_data = {
            'order_info': {
                'order_id': order_data['order_id'],
                'status': order_data['status'],
                'num_of_item': order_data['num_of_item'],
                'created_at': order_data['created_at'],
                'shipped_at': order_data['shipped_at'],
                'delivered_at': order_data['delivered_at'],
                'returned_at': order_data['returned_at']
            },
            'customer_info': {
                'user_id': order_data['user_id'],
                'name': f"{order_data['first_name']} {order_data['last_name']}",
                'email': order_data['email'],
                'shipping_address': {
                    'street': order_data['street_address'],
                    'city': order_data['city'],
                    'state': order_data['state'],
                    'postal_code': order_data['postal_code'],
                    'country': order_data['country']
                }
            }
        }
        
        return jsonify({'order': response_data}), 200
        
    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@app.route('/api/orders', methods=['GET'])
def get_all_orders():
    """Get all orders with pagination and optional filtering."""
    try:
        # Pagination parameters
        page = max(request.args.get('page', default=1, type=int), 1)
        per_page = min(request.args.get('per_page', default=20, type=int), 100)
        offset = (page - 1) * per_page
        
        # Optional status filter
        status_filter = request.args.get('status')
        
        conn = get_db()
        
        # Build query with optional status filter
        base_query = '''
            SELECT  o.order_id,
                    o.user_id,
                    o.status,
                    o.created_at,
                    o.shipped_at,
                    o.delivered_at,
                    o.num_of_item,
                    u.first_name,
                    u.last_name,
                    u.email
            FROM orders AS o
            JOIN users AS u ON o.user_id = u.id
        '''
        
        count_query = 'SELECT COUNT(*) AS c FROM orders AS o JOIN users AS u ON o.user_id = u.id'
        params = []
        
        if status_filter:
            base_query += ' WHERE o.status = ?'
            count_query += ' WHERE o.status = ?'
            params.append(status_filter)
        
        base_query += ' ORDER BY o.created_at DESC LIMIT ? OFFSET ?'
        params.extend([per_page, offset])
        
        # Get total count
        count_params = [status_filter] if status_filter else []
        total_orders = conn.execute(count_query, count_params).fetchone()['c']
        
        # Get orders
        orders = conn.execute(base_query, params).fetchall()
        
        conn.close()
        
        # Calculate pagination info
        total_pages = (total_orders + per_page - 1) // per_page
        
        return jsonify({
            'orders': [row_to_dict(order) for order in orders],
            'filters': {
                'status': status_filter
            },
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_orders': total_orders,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


# ------------------------------------------------------------------
# Error handlers
# ------------------------------------------------------------------
@app.errorhandler(404)
def not_found(_):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def server_error(_):
    return jsonify({'error': 'Internal server error'}), 500


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        print(f'ERROR: Database not found at {DB_PATH}')
        print('Run scripts/database_setup.py first.')
        exit(1)

    print('🚀 Customer & Orders API running at http://localhost:5000')
    print('\n📋 Available endpoints:')
    print('  GET /api/health - Health check')
    print('  GET /api/customers - List customers')
    print('  GET /api/customers/{id} - Customer details')
    print('  GET /api/customers/{id}/orders - Customer orders')
    print('  GET /api/orders - All orders (with filtering)')
    print('  GET /api/orders/{id} - Order details')
    app.run(debug=True, host='0.0.0.0', port=5000)
