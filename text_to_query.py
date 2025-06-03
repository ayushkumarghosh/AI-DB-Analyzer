import sqlite3
import json
from pollinations import StructuredChat
import chromadb
from chromadb.utils import embedding_functions
import os

# Configuration
DB_PATH = "my_database.db"
TABLE_NAME = "sample_sales_data"
VECTOR_DB_PATH = "chroma_db"
COLLECTION_NAME = "docs_collection"
MAX_VALIDATION_ATTEMPTS = 3  # Limit retries to avoid infinite loops

# JSON schema for Python code generation
python_code_schema = {
    "type": "object",
    "properties": {
        "python_code": {"type": "string"}
    },
    "required": ["python_code"]
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
        self.response_chat = StructuredChat(json_schema=response_schema)
        self.code_generator = StructuredChat(json_schema=python_code_schema)
        
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

    def _execute_code_safely(self, code, user_query, context, sql_query, relevant_chunks):
        # Create a local scope for execution with necessary context
        local_vars = {
            "db_path": self.db_path,
            "user_query": user_query,
            "context": context,
            "relevant_chunks": relevant_chunks,
            "sqlite3": sqlite3,
            "json": json
        }
        
        try:
            # Validate database path
            if not self.db_path or not isinstance(self.db_path, (str, bytes)):
                return {
                    "error": "Invalid database path",
                    "sql_query": "",
                    "query_result": [],
                    "validation_history": [user_query]
                }
                
            exec(code, {}, local_vars)
            
            if "result" in local_vars:
                result = local_vars["result"]
                # Ensure result is JSON serializable
                try:
                    # Test serialization
                    json.dumps(result)
                    return result
                except (TypeError, ValueError) as json_err:
                    return {
                        "error": f"Result is not JSON serializable: {str(json_err)}",
                        "sql_query": result.get("sql_query", ""),
                        "query_result": [],
                        "validation_history": [user_query]
                    }
            else:
                return {
                    "error": "Generated code did not produce a 'result' variable",
                    "sql_query": "",
                    "query_result": [],
                    "validation_history": [user_query]
                }
        except Exception as e:
            return {
                "error": f"Error executing generated code: {str(e)}",
                "sql_query": "",
                "query_result": [],
                "validation_history": [user_query]
            }

    def query(self, user_query):
        # Validate database path first
        if not self.db_path or not isinstance(self.db_path, (str, bytes)):
            return {
                "error": "Invalid database path",
                "sql_query": "",
                "query_result": [],
                "validation_history": [user_query]
            }
            
        # Check if database file exists
        if not os.path.exists(self.db_path):
            return {
                "error": f"Database file not found: {self.db_path}",
                "sql_query": "",
                "query_result": [],
                "validation_history": [user_query]
            }
            
        relevant_chunks = self._fetch_relevant_chunks(user_query)
        context = "\nRelevant Documentation Chunks:\n" + "\n".join(relevant_chunks) if relevant_chunks else ""

        # Generate Python code that creates and executes SQL query
        code_prompt = (
            f"Based on the user query '{user_query}', create Python code that:\n"
            f"1. Generates an appropriate SQL query for SQLite based on the user's intent\n"
            f"2. Executes the SQL query against an SQLite database\n"
            f"3. Formats the results based on the query type (e.g., text answer, table data, or visualization)\n"
            f"4. Returns a structured JSON response\n\n"
            f"Additional Context: {context}\n\n"
            f"Use these variables that will be available in the execution environment:\n"
            f"- db_path: Path to the SQLite database (validate this is not None before using)\n"
            f"- user_query: The original user query\n"
            f"- context: Relevant documentation chunks\n"
            f"- relevant_chunks: List of relevant document chunks\n"
            f"- sqlite3: The sqlite3 module\n"
            f"- json: The json module\n\n"
            f"The code should:\n"
            f"- Validate db_path exists and is not None\n"
            f"- Create a valid SQL query based on the user's request\n"
            f"- Connect to the database at db_path\n"
            f"- Execute the SQL query\n"
            f"- Handle any SQLite errors with try-except blocks\n"
            f"- Analyze the query intent and results to determine the appropriate response format:\n"
            f"  * Text answer for simple facts or explanations\n"
            f"  * Table data (columns and rows) for detailed records\n"
            f"  * Graph data (bar, line, or pie) for trends or comparisons\n"
            f"- Return a JSON object as a variable named 'result' with these fields:\n"
            f"  * 'answer': String with text response (if applicable)\n"
            f"  * 'table_data': Object with 'columns' and 'rows' (if applicable)\n"
            f"  * 'graph_data': Object with 'type', 'labels', 'values', 'title' (if applicable)\n"
            f"  * 'sql_query': The generated and executed SQL query\n"
            f"  * 'query_result': Raw query results (ensure this is JSON serializable)\n"
            f"  * 'validation_history': List with the original query\n"
            f"  * 'relevant_chunks': The documentation chunks used\n\n"
            f"Ensure the SQL query follows SQLite syntax rules (e.g., ORDER BY and LIMIT in UNION ALL must be within subqueries or after the entire UNION ALL).\n"
            f"For queries asking for extremes (highest/lowest), use appropriate subqueries or CTEs if combining results.\n"
            f"Ensure all objects in the result are properly JSON serializable (no complex objects).\n"
            f"Ensure the code is self-contained and handles errors gracefully."
        )
        
        code_response = self.code_generator.send_message(code_prompt)
        
        if not code_response or "python_code" not in code_response:
            return {
                "error": "Failed to generate Python code", 
                "details": code_response,
                "sql_query": "",
                "query_result": [],
                "validation_history": [user_query]
            }
        
        generated_code = code_response["python_code"]
        
        # Execute the generated Python code
        result = self._execute_code_safely(
            generated_code, 
            user_query, 
            context, 
            "", # Passing empty string for sql_query since it's now generated in the code
            relevant_chunks
        )
        
        return result
    
# def main():
#     t2q = TextToQuery(DB_PATH, TABLE_NAME)
    
#     queries = [
#         "What are the orders with the highest and lowest total amounts after applying the highest discount?", #Tests handling of the Discount_Percentage constraint (0-100), the Total_Amount calculation, and extreme values.
#         "Show all pending orders in the North region where shipping cost exceeds 30 dollars, sorted by total amount descending.", #Tests multi-condition filtering (Order_Status, Region, Shipping_Cost), sorting, and natural language understanding of thresholds.
#         "What's the average unit price for each product category, excluding orders with zero discount?", #Tests aggregation (AVG), grouping (Category), and exclusion based on a condition (Discount_Percentage).
#         "Which completed orders from the last 30 days have a total amount greater than 1000 dollars?", #Tests date arithmetic (relative to current date, March 23, 2025), filtering on Order_Status and Total_Amount, and natural language date interpretation.
#         "Find orders where the discount reduces the total unit price cost to less than the shipping cost.", #Tests understanding of the Total_Amount formula, comparing its components (Unit_Price * Quantity * (1 - Discount_Percentage/100) vs. Shipping_Cost), and edge case arithmetic.,
#     ]
    
#     with open("output.txt", "w") as output_file:
#         for query in queries:
#             response = t2q.query(query)
#             output_file.write(f"Query: {query}\n")
#             output_file.write(f"Response: {json.dumps(response, indent=2)}\n\n")

# if __name__ == "__main__":
#     main()