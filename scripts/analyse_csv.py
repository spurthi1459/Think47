import pandas as pd
import os

def analyze_csv_structure():
    # Get the parent directory to look for CSV files
    parent_dir = os.path.dirname(os.getcwd())
    data_dir = os.path.join(parent_dir, 'data')
    
    print(f"Looking for CSV files in: {data_dir}")
    print(f"Current working directory: {os.getcwd()}")
    print("-" * 50)
    
    # Check if data directory exists
    if not os.path.exists(data_dir):
        print(f"Data directory not found: {data_dir}")
        print("Please ensure your CSV files are in the correct location.")
        return
    
    # Analyze users.csv structure
    users_file = os.path.join(data_dir, 'users.csv')
    try:
        users_df = pd.read_csv(users_file)
        print("✓ USERS CSV ANALYSIS")
        print(f"File: {users_file}")
        print(f"Shape: {users_df.shape}")
        print(f"Columns: {list(users_df.columns)}")
        print(f"Data types:\n{users_df.dtypes}")
        print(f"Sample data:\n{users_df.head(3)}")
        print(f"Null values:\n{users_df.isnull().sum()}")
        print("\n" + "="*60 + "\n")
    except FileNotFoundError:
        print(f"❌ users.csv not found at: {users_file}")
    except Exception as e:
        print(f"❌ Error reading users.csv: {e}")

    # Analyze orders.csv structure
    orders_file = os.path.join(data_dir, 'orders.csv')
    try:
        orders_df = pd.read_csv(orders_file)
        print("✓ ORDERS CSV ANALYSIS")
        print(f"File: {orders_file}")
        print(f"Shape: {orders_df.shape}")
        print(f"Columns: {list(orders_df.columns)}")
        print(f"Data types:\n{orders_df.dtypes}")
        print(f"Sample data:\n{orders_df.head(3)}")
        print(f"Null values:\n{orders_df.isnull().sum()}")
        print("\n" + "="*60 + "\n")
    except FileNotFoundError:
        print(f"❌ orders.csv not found at: {orders_file}")
    except Exception as e:
        print(f"❌ Error reading orders.csv: {e}")
    
    # List all files in data directory
    try:
        files_in_data = os.listdir(data_dir)
        print(f"Files found in data directory: {files_in_data}")
    except:
        print("Could not list files in data directory")

if __name__ == "__main__":
    analyze_csv_structure()
