-- Check record counts
SELECT 'Users' as table_name, COUNT(*) as record_count FROM users
UNION ALL
SELECT 'Orders' as table_name, COUNT(*) as record_count FROM orders;

-- Sample data from users
SELECT 'Sample Users:' as info;
SELECT * FROM users LIMIT 5;

-- Sample data from orders
SELECT 'Sample Orders:' as info;
SELECT * FROM orders LIMIT 5;

-- User order summary
SELECT 
    u.username,
    u.email,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_spent
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.username, u.email
ORDER BY total_spent DESC
LIMIT 10;
