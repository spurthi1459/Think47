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

    print('🚀 Customer API running at http://localhost:5000')
    app.run(debug=True, host='0.0.0.0', port=5000)
