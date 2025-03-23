import sqlite3
import pandas as pd
import chromadb
from sentence_transformers import SentenceTransformer
from pollinations import StructuredChat
import json

# Configuration
DB_PATH = "my_database.db"
TABLE_NAME = "sample_sales_data"
CHUNK_SIZE = 500  # Characters per chunk
TOP_K = 10  # Number of relevant chunks to retrieve

# JSON schema for RAG responses
rag_schema = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "relevant_data": {
            "type": "array",
            "items": {"type": "string"}
        }
    },
    "required": ["answer", "relevant_data"]
}

class RAGSystem:
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name
        self.vector_client = chromadb.Client()
        self.collection = self.vector_client.create_collection(name=f"{table_name}_vectors")
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')  # Lightweight embedding model
        self.chat = StructuredChat(json_schema=rag_schema)
        
        # Initialize vector DB
        self._initialize_vector_db()

    def _fetch_table_data(self):
        """Fetch all data from the SQLite table."""
        print("fetchin table data...")
        conn = sqlite3.connect(self.db_path)
        query = f"SELECT * FROM {self.table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def _chunk_text(self, text, chunk_size=CHUNK_SIZE):
        """Split text into chunks of specified size."""
        return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    def _initialize_vector_db(self):
        """Convert table data to text chunks, embed, and store in vector DB."""
        df = self._fetch_table_data()
        
        # Convert each row to a string representation
        print("chunking data...")
        documents = []
        ids = []
        for idx, row in df.iterrows():
            row_text = " ".join([f"{col}: {str(val)}" for col, val in row.items()])
            chunks = self._chunk_text(row_text)
            for chunk_idx, chunk in enumerate(chunks):
                documents.append(chunk)
                ids.append(f"{idx}_{chunk_idx}")
        
        # Generate embeddings
        print("generating embeddings...")
        embeddings = self.embedder.encode(documents, convert_to_tensor=False).tolist()
        
        # Store in ChromaDB
        self.collection.add(
            documents=documents,
            embeddings=embeddings,
            ids=ids
        )
        print(f"Initialized vector DB with {len(documents)} chunks from {self.table_name}")

    def query(self, question):
        """Implement RAG: Retrieve relevant chunks and generate structured response."""
        # Embed the question
        question_embedding = self.embedder.encode([question], convert_to_tensor=False).tolist()[0]
        
        # Retrieve top-k relevant chunks
        results = self.collection.query(
            query_embeddings=[question_embedding],
            n_results=TOP_K
        )
        
        relevant_chunks = results['documents'][0]
        context = "\n".join(relevant_chunks)
        
        # Construct prompt for StructuredChat
        prompt = (
            f"Based on the following data from the {self.table_name} table:\n"
            f"{context}\n\n"
            f"Answer this question: {question}"
        )
        
        # Get structured response
        response = self.chat.send_message(prompt)
        return response

def main():
    # Initialize RAG system
    rag = RAGSystem(DB_PATH, TABLE_NAME)
    
    # Example queries
    queries = [
        "What’s the average shipping cost for wireless mice?",
        "Which product has the highest total amount?",
        "Summarize sales in the North region."
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        response = rag.query(query)
        print("Response:", json.dumps(response, indent=2))

if __name__ == "__main__":
    main()