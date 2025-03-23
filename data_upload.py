import sqlite3
import pandas as pd
import os

def create_database_from_csv(csv_file_path, conn, table_name):
    """
    Load a CSV file into a SQLite database table
    
    Parameters:
    csv_file_path (str): Path to the CSV file
    conn: SQLite database connection
    table_name (str): Name of the table
    """
    try:
        # Read CSV file using pandas
        df = pd.read_csv(csv_file_path)
        
        # Replace spaces and special characters in column names with underscores
        df.columns = [col.replace(' ', '_').replace('.', '_').replace('-', '_') 
                     for col in df.columns]
        
        # Write the data to SQLite table
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"Loaded {csv_file_path} into table: {table_name}")
        
        # Verify the data
        cursor = conn.cursor()
        cursor.execute(f"SELECT count(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Number of rows in {table_name}: {row_count}")
        
    except Exception as e:
        print(f"Error loading {csv_file_path}: {str(e)}")

def load_all_csv_from_folder(folder_path, db_name):
    """
    Load all CSV files from a folder into separate tables in a SQLite database
    
    Parameters:
    folder_path (str): Path to folder containing CSV files
    db_name (str): Name of the SQLite database file
    """
    # Check if folder exists
    if not os.path.exists(folder_path):
        print(f"Error: Folder {folder_path} not found")
        return
    
    # Create connection to database
    conn = sqlite3.connect(db_name)
    
    try:
        # Get all CSV files in the folder
        csv_files = [f for f in os.listdir(folder_path) 
                    if f.lower().endswith('.csv')]
        
        if not csv_files:
            print(f"No CSV files found in {folder_path}")
            return
        
        # Process each CSV file
        for csv_file in csv_files:
            csv_path = os.path.join(folder_path, csv_file)
            # Use filename without extension as table name
            table_name = os.path.splitext(csv_file)[0]
            create_database_from_csv(csv_path, conn, table_name)
        
        print(f"\nFinished loading {len(csv_files)} CSV files into {db_name}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
    finally:
        conn.close()

def main():
    # Set folder path and database name
    folder_path = "data"
    database_name = "my_database.db"
    
    # Load all CSV files
    load_all_csv_from_folder(folder_path, database_name)

if __name__ == "__main__":
    main()