import sqlite3
import pandas as pd
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EcommerceDatabase:
    def __init__(self, db_name='../database/ecommerce.db'):
        self.db_name = db_name
        self.connection = None
        # Set up paths relative to scripts directory
        self.data_dir = '../data'
    
    def connect(self):
        """Create database connection"""
        try:
            # Create database directory if it doesn't exist
            db_dir = os.path.dirname(self.db_name)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            self.connection = sqlite3.connect(self.db_name)
            logging.info(f"Connected to {self.db_name}")
            return True
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            return False
    
    def create_tables(self):
        """Create users and orders tables based on actual CSV structure"""
        if not self.connection:
            logging.error("No database connection")
            return False
        
        cursor = self.connection.cursor()
        
        # Create users table based on your CSV structure
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT UNIQUE,
                age INTEGER,
                gender TEXT,
                state TEXT,
                street_address TEXT,
                postal_code TEXT,
                city TEXT,
                country TEXT,
                latitude REAL,
                longitude REAL,
                traffic_source TEXT,
                created_at TEXT
            )
        ''')
        
        # Create orders table based on your CSV structure
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                status TEXT,
                gender TEXT,
                created_at TEXT,
                returned_at TEXT,
                shipped_at TEXT,
                delivered_at TEXT,
                num_of_item INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_id ON users(id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)')
        
        self.connection.commit()
        logging.info("Tables created successfully")
        return True
    
    def load_users_csv(self, csv_file='users.csv'):
        """Load users data from CSV with correct file path"""
        try:
            # Construct full path to CSV file
            csv_path = os.path.join(self.data_dir, csv_file)
            
            if not os.path.exists(csv_path):
                logging.error(f"CSV file not found: {csv_path}")
                return False
            
            # Read CSV with all columns matching your structure
            df = pd.read_csv(csv_path)
            logging.info(f"Reading {len(df)} users from {csv_path}")
            
            # Load data into SQLite (this will map columns automatically)
            df.to_sql('users', self.connection, if_exists='replace', index=False)
            logging.info(f"Successfully loaded {len(df)} users into database")
            return True
            
        except Exception as e:
            logging.error(f"Error loading users CSV: {e}")
            return False
    
    def load_orders_csv(self, csv_file='orders.csv'):
        """Load orders data from CSV with correct file path"""
        try:
            # Construct full path to CSV file
            csv_path = os.path.join(self.data_dir, csv_file)
            
            if not os.path.exists(csv_path):
                logging.error(f"CSV file not found: {csv_path}")
                return False
            
            # Read CSV with all columns matching your structure
            df = pd.read_csv(csv_path)
            logging.info(f"Reading {len(df)} orders from {csv_path}")
            
            # Load data into SQLite (this will map columns automatically)
            df.to_sql('orders', self.connection, if_exists='replace', index=False)
            logging.info(f"Successfully loaded {len(df)} orders into database")
            return True
            
        except Exception as e:
            logging.error(f"Error loading orders CSV: {e}")
            return False
    
    def verify_data(self):
        """Verify loaded data with detailed analysis"""
        cursor = self.connection.cursor()
        
        print("\n" + "="*60)
        print("DATABASE VERIFICATION REPORT")
        print("="*60)
        
        # Check users
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        print(f"📊 Total users loaded: {user_count:,}")
        
        # Sample users data
        cursor.execute("SELECT id, first_name, last_name, email, city, country FROM users LIMIT 5")
        print("\n👥 Sample users:")
        for row in cursor.fetchall():
            print(f"  ID: {row[0]}, Name: {row[1]} {row[2]}, Email: {row[3]}, Location: {row[4]}, {row[5]}")
        
        # Check orders
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        print(f"\n📦 Total orders loaded: {order_count:,}")
        
        # Sample orders data
        cursor.execute("SELECT order_id, user_id, status, num_of_item, created_at FROM orders LIMIT 5")
        print("\n🛒 Sample orders:")
        for row in cursor.fetchall():
            print(f"  Order: {row[0]}, User: {row[1]}, Status: {row[2]}, Items: {row[3]}, Date: {row[4]}")
        
        # Order status breakdown
        cursor.execute("SELECT status, COUNT(*) as count FROM orders GROUP BY status ORDER BY count DESC")
        print("\n📈 Order status breakdown:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]:,} orders")
        
        # Top customers by order count
        cursor.execute("""
            SELECT u.id, u.first_name, u.last_name, u.email, 
                   COUNT(o.order_id) as total_orders,
                   SUM(o.num_of_item) as total_items
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id, u.first_name, u.last_name, u.email
            HAVING total_orders > 0
            ORDER BY total_orders DESC
            LIMIT 10
        """)
        print("\n🏆 Top 10 customers by order count:")
        for row in cursor.fetchall():
            print(f"  {row[1]} {row[2]} ({row[3]}): {row[4]} orders, {row[5]} total items")
        
        # Data integrity check
        cursor.execute("""
            SELECT COUNT(*) as orphaned_orders 
            FROM orders o 
            LEFT JOIN users u ON o.user_id = u.id 
            WHERE u.id IS NULL
        """)
        orphaned = cursor.fetchone()[0]
        print(f"\n🔍 Data integrity check:")
        print(f"  Orphaned orders (orders without valid user): {orphaned}")
        
        # Geographic distribution
        cursor.execute("""
            SELECT country, COUNT(*) as user_count 
            FROM users 
            WHERE country IS NOT NULL 
            GROUP BY country 
            ORDER BY user_count DESC 
            LIMIT 10
        """)
        print(f"\n🌍 Top 10 countries by user count:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]:,} users")
        
        print("\n" + "="*60)
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed")

def main():
    print("🚀 Starting ecommerce database setup...")
    
    # Initialize database
    db = EcommerceDatabase()
    
    if db.connect():
        try:
            # Create tables
            print("📋 Creating database tables...")
            db.create_tables()
            
            # Load data
            print("📥 Loading users data...")
            users_loaded = db.load_users_csv()
            
            print("📥 Loading orders data...")
            orders_loaded = db.load_orders_csv()
            
            # Verify data only if both loads were successful
            if users_loaded and orders_loaded:
                print("✅ Data loading completed successfully!")
                db.verify_data()
            else:
                print("❌ Data loading failed. Check the error messages above.")
            
        except Exception as e:
            logging.error(f"Error in main process: {e}")
        finally:
            # Always close connection
            db.close()
    else:
        print("❌ Failed to connect to database")

if __name__ == "__main__":
    main()
