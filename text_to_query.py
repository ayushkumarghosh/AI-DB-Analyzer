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
MAX_VALIDATION_ATTEMPTS = 3  # Limit retries to avoid infinite loops

# JSON schema for SQL query generation
sql_schema = {
    "type": "object",
    "properties": {
        "sql_query": {"type": "string"}
    },
    "required": ["sql_query"]
}

# JSON schema for SQL validation/correction
sql_validation_schema = {
    "type": "object",
    "properties": {
        "is_valid": {"type": "boolean"},
        "corrected_sql_query": {"type": "string"},
        "reason": {"type": "string"}
    },
    "required": ["is_valid", "corrected_sql_query", "reason"]
}

# Response schema
response_schema = {
    "type": "object",
    "properties": {
        "answer": {"type": "string", "description": "Textual answer to the user's query (optional)"},
        "table_data": {
            "type": "object",
            "properties": {
                "columns": {"type": "array", "items": {"type": "string"}},
                "rows": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {"type": ["string", "number", "boolean", "null"]}
                    }
                }
            },
            "description": "Structured table data (optional)"
        },
        "graph_data": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["bar", "line", "pie"]},
                "labels": {"type": "array", "items": {"type": "string"}},
                "values": {"type": "array", "items": {"type": "number"}},
                "title": {"type": "string"}
            },
            "description": "Data for graphical representation (optional)"
        },
        "sql_query": {"type": "string"},
        "query_result": {"type": "array", "items": {"type": "object"}},
        "validation_history": {"type": "array", "items": {"type": "string"}}  # Track validation attempts
    },
    "required": ["sql_query", "query_result"]
}

class TextToQuery:
    def __init__(self, db_path, table_name, vector_db_path=VECTOR_DB_PATH):
        self.db_path = db_path
        self.table_name = table_name
        self.sql_chat = StructuredChat(json_schema=sql_schema)
        self.sql_validator = StructuredChat(json_schema=sql_validation_schema)
        self.response_chat = StructuredChat(json_schema=response_schema)
        
        # self.table_schema = self._get_table_schema()
        
        self.vector_client = chromadb.PersistentClient(path=vector_db_path)
        # self.embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
        self.collection = self.vector_client.get_collection(name=COLLECTION_NAME)

    # def _get_table_schema(self):
    #     conn = sqlite3.connect(self.db_path)
    #     cursor = conn.cursor()
    #     cursor.execute(f"PRAGMA table_info({self.table_name})")
    #     columns = cursor.fetchall()
    #     conn.close()
        
    #     schema_desc = "Table: {}\nColumns:\n".format(self.table_name)
    #     for col in columns:
    #         schema_desc += f"- {col[1]} ({col[2]})\n"
    #     return schema_desc

    def _execute_sql(self, sql_query):
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
        try:
            results = self.collection.query(
                query_texts=[query],
                n_results=n_results
            )
            return results['documents'][0] if results['documents'] else []
        except Exception as e:
            print(f"Error fetching from vector DB: {str(e)}")
            return []

    def _validate_and_correct_sql(self, sql_query, user_query, context, previous_attempts=""):
        validation_prompt = (
            # f"Given the following table schema:\n"
            # f"{self.table_schema}\n"
            f"Given the following details:\n"
            f"{context}\n"
            f"Original user query: '{user_query}'\n"
            f"Generated SQL query to validate: '{sql_query}'\n"
            f"Previous validation attempts (if any): {previous_attempts}\n\n"
            f"Validate this SQL query for SQLite syntax correctness and intent alignment:\n"
            f"- Ensure it follows SQLite rules: e.g., ORDER BY and LIMIT in UNION ALL must be within subqueries or after the entire UNION ALL.\n"
            f"- Check for errors like misplaced clauses, incorrect aggregation, or syntax issues.\n"
            f"- If invalid, provide a corrected query that achieves the same intent as '{user_query}'.\n"
            f"- Return a JSON object with 'is_valid' (boolean), 'corrected_sql_query' (string), and 'reason' (string explanation).\n"
            f"Example correction for UNION ALL with ORDER BY:\n"
            f"  Invalid: SELECT ... ORDER BY ... LIMIT 1 UNION ALL SELECT ... ORDER BY ... LIMIT 1\n"
            f"  Valid: SELECT * FROM (SELECT ... ORDER BY ... LIMIT 1) UNION ALL SELECT * FROM (SELECT ... ORDER BY ... LIMIT 1)"
        )
        validation_response = self.sql_validator.send_message(validation_prompt)
        
        return validation_response

    def query(self, user_query):
        relevant_chunks = self._fetch_relevant_chunks(user_query)
        context = "\nRelevant Documentation Chunks:\n" + "\n".join(relevant_chunks) if relevant_chunks else ""

        # Step 1: Generate initial SQL query
        sql_prompt = (
            # f"Given the following table schema:\n"
            # f"{self.table_schema}\n"
            f"Given the following details:\n"
            f"{context}\n"
            f"Convert this natural language query into a valid SQL query for SQLite:\n"
            f"'{user_query}'\n"
            f"Rules:\n"
            f"- For queries asking for extremes (e.g., highest/lowest), use subqueries or CTEs if combining results with UNION ALL.\n"
            f"- Place ORDER BY and LIMIT after UNION ALL only if sorting the combined result; otherwise, wrap individual SELECTs in subqueries.\n"
            f"- Ensure the query is syntactically correct for SQLite.\n"
            f"Return only the SQL query in the JSON response."
        )
        sql_response = self.sql_chat.send_message(sql_prompt)
        
        if not sql_response or "sql_query" not in sql_response:
            return {"error": "Failed to generate SQL query", "details": sql_response}
        
        current_sql_query = sql_response["sql_query"]
        validation_history = [f"Initial query: {current_sql_query}"]

        # Step 2: Continuous validation loop
        for attempt in range(MAX_VALIDATION_ATTEMPTS):
            # Validate the current query
            validation_result = self._validate_and_correct_sql(
                current_sql_query, user_query, context, 
                previous_attempts="\n".join(validation_history)
            )
            
            if not validation_result or "is_valid" not in validation_result:
                return {"error": "Failed to validate SQL query", "details": validation_result}
            
            is_valid = validation_result["is_valid"]
            current_sql_query = validation_result["corrected_sql_query"]
            reason = validation_result["reason"]
            validation_history.append(f"Attempt {attempt + 1}: {reason} - Query: {current_sql_query}")

            # Try executing the query
            query_result = self._execute_sql(current_sql_query)
            
            if not isinstance(query_result, dict) or "error" not in query_result:
                # Success! Query executed without error
                break
            else:
                # Query failed, log the error and continue validation
                validation_history.append(f"Execution failed: {query_result['error']}")

        else:
            # Max attempts reached, return the last error
            return {
                "error": "SQL execution failed after maximum validation attempts",
                "details": query_result["error"],
                "sql_query": current_sql_query,
                "validation_history": validation_history
            }

        # Step 3: Generate multi-type response
        response_prompt = (
            f"Based on the following SQL query and its results from the {self.table_name} table:\n"
            f"SQL Query: {current_sql_query}\n"
            f"Results: {json.dumps(query_result, indent=2)}\n"
            f"Additional Context from Documentation:\n{context}\n"
            f"Validation History: {json.dumps(validation_history, indent=2)}\n\n"
            f"Provide a response to the user's original question: '{user_query}'\n"
            f"Determine the best response format(s) based on the query's intent and data:\n"
            f"- Use 'answer' for a concise textual summary if the query asks for a simple fact or explanation.\n"
            f"- Use 'table_data' (with 'columns' and 'rows') if the query requests detailed records or a list.\n"
            f"- Use 'graph_data' (with 'type', 'labels', 'values', 'title') if the query involves trends, comparisons, or aggregations suitable for visualization.\n"
            f"A single response can include multiple types (e.g., 'answer' and 'table_data', or all three).\n"
            f"Always include 'sql_query', 'query_result', 'relevant_chunks', and 'validation_history'."
        )
        final_response = self.response_chat.send_message(response_prompt)
        
        if "relevant_chunks" not in final_response:
            final_response["relevant_chunks"] = relevant_chunks
        if "validation_history" not in final_response:
            final_response["validation_history"] = validation_history
        
        return final_response
    
def main():
    t2q = TextToQuery(DB_PATH, TABLE_NAME)
    
    queries = [
        "What are the orders with the highest and lowest total amounts after applying the highest discount?", #Tests handling of the Discount_Percentage constraint (0-100), the Total_Amount calculation, and extreme values.
        "Show all pending orders in the North region where shipping cost exceeds 30 dollars, sorted by total amount descending.", #Tests multi-condition filtering (Order_Status, Region, Shipping_Cost), sorting, and natural language understanding of thresholds.
        "What’s the average unit price for each product category, excluding orders with zero discount?", #Tests aggregation (AVG), grouping (Category), and exclusion based on a condition (Discount_Percentage).
        "Which completed orders from the last 30 days have a total amount greater than 1000 dollars?", #Tests date arithmetic (relative to current date, March 23, 2025), filtering on Order_Status and Total_Amount, and natural language date interpretation.
        "Find orders where the discount reduces the total unit price cost to less than the shipping cost.", #Tests understanding of the Total_Amount formula, comparing its components (Unit_Price * Quantity * (1 - Discount_Percentage/100) vs. Shipping_Cost), and edge case arithmetic.,
    ]
    
    with open("output.txt", "w") as output_file:
        for query in queries:
            response = t2q.query(query)
            output_file.write(f"Query: {query}\n")
            output_file.write(f"Response: {json.dumps(response, indent=2)}\n\n")

if __name__ == "__main__":
    main()