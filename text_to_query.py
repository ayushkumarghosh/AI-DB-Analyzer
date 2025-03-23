__import__('pysqlite3')
import sys
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
import sqlite3
import json
from pollinations import StructuredChat
import chromadb
from chromadb.utils import embedding_functions

# Configuration
DB_PATH = "my_database.db"
TABLE_NAME = "sample_sales_data"
VECTOR_DB_PATH = "chroma_db"
COLLECTION_NAME = "docs_collection"

# JSON schema for SQL query generation
sql_schema = {
    "type": "object",
    "properties": {
        "sql_query": {"type": "string"}
    },
    "required": ["sql_query"]
}

# Updated JSON schema for final response to include relevant_chunks
response_schema = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "sql_query": {"type": "string"},
        "query_result": {
            "type": "array",
            "items": {"type": "object"}
        },
        "relevant_chunks": {
            "type": "array",
            "items": {"type": "string"}
        }
    },
    "required": ["answer", "sql_query", "query_result", "relevant_chunks"]
}

class TextToQuery:
    def __init__(self, db_path, table_name, vector_db_path=VECTOR_DB_PATH):
        self.db_path = db_path
        self.table_name = table_name
        self.sql_chat = StructuredChat(json_schema=sql_schema)
        self.response_chat = StructuredChat(json_schema=response_schema)
        
        # Get table schema for context
        self.table_schema = self._get_table_schema()
        
        # Initialize ChromaDB client
        self.vector_client = chromadb.PersistentClient(path=vector_db_path)
        self.embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
        self.collection = self.vector_client.get_collection(name=COLLECTION_NAME)

    def _get_table_schema(self):
        """Fetch the schema of the table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.table_name})")
        columns = cursor.fetchall()
        conn.close()
        
        schema_desc = "Table: {}\nColumns:\n".format(self.table_name)
        for col in columns:
            schema_desc += f"- {col[1]} ({col[2]})\n"
        return schema_desc

    def _execute_sql(self, sql_query):
        """Execute the SQL query and return results."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            return {"error": str(e)}

    def _fetch_relevant_chunks(self, query, n_results=3):
        """Fetch relevant chunks from vector DB based on user query."""
        try:
            results = self.collection.query(
                query_texts=[query],
                n_results=n_results
            )
            return results['documents'][0] if results['documents'] else []
        except Exception as e:
            print(f"Error fetching from vector DB: {str(e)}")
            return []

    def query(self, user_query):
        """Convert user query to SQL, execute it, and generate structured response with vector DB context."""
        # Step 1: Fetch relevant chunks from vector DB
        relevant_chunks = self._fetch_relevant_chunks(user_query)
        context = "\nRelevant Documentation Chunks:\n" + "\n".join(relevant_chunks) if relevant_chunks else ""

        # Step 2: Convert user query to SQL with additional context
        sql_prompt = (
            f"Given the following table schema:\n"
            f"{self.table_schema}\n"
            f"{context}\n"
            f"Convert this natural language query into a valid SQL query for SQLite:\n"
            f"'{user_query}'\n"
            f"Return only the SQL query in the JSON response."
        )
        sql_response = self.sql_chat.send_message(sql_prompt)
        
        if not sql_response or "sql_query" not in sql_response:
            return {"error": "Failed to generate SQL query", "details": sql_response}
        
        sql_query = sql_response["sql_query"]
        
        # Step 3: Execute the SQL query
        query_result = self._execute_sql(sql_query)
        
        if isinstance(query_result, dict) and "error" in query_result:
            return {"error": "SQL execution failed", "details": query_result["error"], "sql_query": sql_query}
        
        # Step 4: Generate final response with query result, vector DB context, and chunks in output
        response_prompt = (
            f"Based on the following SQL query and its results from the {self.table_name} table:\n"
            f"SQL Query: {sql_query}\n"
            f"Results: {json.dumps(query_result, indent=2)}\n"
            f"Additional Context from Documentation:\n{context}\n\n"
            f"Provide an answer to the user's original question: '{user_query}'\n"
            f"Also include the relevant documentation chunks in the response under 'relevant_chunks'."
        )
        final_response = self.response_chat.send_message(response_prompt)
        
        # Ensure relevant_chunks is included even if the response_chat doesn't add it explicitly
        if "relevant_chunks" not in final_response:
            final_response["relevant_chunks"] = relevant_chunks
        
        return final_response

def main():
    t2q = TextToQuery(DB_PATH, TABLE_NAME)
    
    queries = [
        "What are the orders with the highest and lowest total amounts after applying the maximum and minimum discounts?", #Tests handling of the Discount_Percentage constraint (0-100), the Total_Amount calculation, and extreme values.
        "Show all pending orders in the North region where shipping cost exceeds 50 dollars, sorted by total amount descending.", #Tests multi-condition filtering (Order_Status, Region, Shipping_Cost), sorting, and natural language understanding of thresholds.
        "What’s the average unit price for each product category, excluding orders with zero discount?", #Tests aggregation (AVG), grouping (Category), and exclusion based on a condition (Discount_Percentage).
        "Which completed orders from the last 30 days have a total amount greater than 1000 dollars?", #Tests date arithmetic (relative to current date, March 23, 2025), filtering on Order_Status and Total_Amount, and natural language date interpretation.
        "Find orders where the discount reduces the total unit price cost to less than the shipping cost." #Tests understanding of the Total_Amount formula, comparing its components (Unit_Price * Quantity * (1 - Discount_Percentage/100) vs. Shipping_Cost), and edge case arithmetic.
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        response = t2q.query(query)
        print("Response:", json.dumps(response, indent=2))

if __name__ == "__main__":
    main()