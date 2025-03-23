import sqlite3
import json
from pollinations import StructuredChat

# Configuration
DB_PATH = "my_database.db"
TABLE_NAME = "sample_sales_data"

# JSON schema for SQL query generation
sql_schema = {
    "type": "object",
    "properties": {
        "sql_query": {"type": "string"}
    },
    "required": ["sql_query"]
}

# JSON schema for final response
response_schema = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "sql_query": {"type": "string"},
        "query_result": {
            "type": "array",
            "items": {"type": "object"}
        }
    },
    "required": ["answer", "sql_query", "query_result"]
}

class TextToQuery:
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name
        self.sql_chat = StructuredChat(json_schema=sql_schema)
        self.response_chat = StructuredChat(json_schema=response_schema)
        
        # Get table schema for context
        self.table_schema = self._get_table_schema()

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

    def query(self, user_query):
        """Convert user query to SQL, execute it, and generate structured response."""
        # Step 1: Convert user query to SQL
        sql_prompt = (
            f"Given the following table schema:\n"
            f"{self.table_schema}\n"
            f"Convert this natural language query into a valid SQL query for SQLite:\n"
            f"'{user_query}'\n"
            f"Return only the SQL query in the JSON response."
        )
        sql_response = self.sql_chat.send_message(sql_prompt)
        
        if not sql_response or "sql_query" not in sql_response:
            return {"error": "Failed to generate SQL query", "details": sql_response}
        
        sql_query = sql_response["sql_query"]
        
        # Step 2: Execute the SQL query
        query_result = self._execute_sql(sql_query)
        
        if isinstance(query_result, dict) and "error" in query_result:
            return {"error": "SQL execution failed", "details": query_result["error"], "sql_query": sql_query}
        
        # Step 3: Generate final response
        response_prompt = (
            f"Based on the following SQL query and its results from the {self.table_name} table:\n"
            f"SQL Query: {sql_query}\n"
            f"Results: {json.dumps(query_result, indent=2)}\n\n"
            f"Provide an answer to the user's original question: '{user_query}'"
        )
        final_response = self.response_chat.send_message(response_prompt)
        
        return final_response

def main():
    # Initialize TextToQuery system
    t2q = TextToQuery(DB_PATH, TABLE_NAME)
    
    # Example queries
    queries = [
        "What’s the average total amount for Mouse?",
        "Which product has the highest total amount?",
        "Summarize sales in the North region."
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        response = t2q.query(query)
        print("Response:", json.dumps(response, indent=2))

if __name__ == "__main__":
    main()