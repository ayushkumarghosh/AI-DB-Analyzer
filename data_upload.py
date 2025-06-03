import sqlite3
import pandas as pd
import os
import chromadb
from chromadb.utils import embedding_functions
import uuid

def create_database_from_csv(csv_file_path, conn, table_name):
    """Load a CSV file into a SQLite database table"""
    try:
        df = pd.read_csv(csv_file_path)
        df.columns = [col.replace(' ', '_').replace('.', '_').replace('-', '_') 
                     for col in df.columns]
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Loaded {csv_file_path} into table: {table_name}")
        cursor = conn.cursor()
        cursor.execute(f"SELECT count(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Number of rows in {table_name}: {row_count}")
    except Exception as e:
        print(f"Error loading {csv_file_path}: {str(e)}")

def create_vector_db_from_files(folder_path, client, collection_name="docs_collection"):
    """Create vector DB from documentation.txt and db_schema.sql"""
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    collection = client.get_or_create_collection(name=collection_name, embedding_function=ef)
    
    doc_files = ['documentation.txt', 'db_schema.sql']
    for file_name in doc_files:
        file_path = os.path.join(folder_path, file_name)
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Split content into chunks (simple split by paragraph/newline for now)
                chunks = [chunk.strip() for chunk in content.split('\n\n') if chunk.strip()]
                
                # Add chunks to vector DB
                for chunk in chunks:
                    chunk_id = str(uuid.uuid4())
                    collection.add(
                        documents=[chunk],
                        metadatas=[{"source": file_name}],
                        ids=[chunk_id]
                    )
            print(f"Added {file_name} to vector DB with {len(chunks)} chunks")
        else:
            print(f"Warning: {file_name} not found in {folder_path}")

def load_all_csv_and_docs_from_folder(folder_path, db_name, vector_db_path="chroma_db"):
    """Load all CSV files and documentation into SQLite and vector DB"""
    if not os.path.exists(folder_path):
        print(f"Error: Folder {folder_path} not found")
        return
    
    # SQLite connection
    conn = sqlite3.connect(db_name)
    
    # ChromaDB client
    client = chromadb.PersistentClient(path=vector_db_path)
    
    try:
        # Process CSV files
        csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]
        if not csv_files:
            print(f"No CSV files found in {folder_path}")
        else:
            for csv_file in csv_files:
                csv_path = os.path.join(folder_path, csv_file)
                table_name = os.path.splitext(csv_file)[0]
                create_database_from_csv(csv_path, conn, table_name)
        
        # Process documentation files into vector DB
        create_vector_db_from_files(folder_path, client)
        
        print(f"\nFinished loading {len(csv_files)} CSV files into {db_name} and docs into vector DB")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
    finally:
        conn.close()

def main():
    folder_path = "data"
    database_name = "my_database.db"
    load_all_csv_and_docs_from_folder(folder_path, database_name)

if __name__ == "__main__":
    main()