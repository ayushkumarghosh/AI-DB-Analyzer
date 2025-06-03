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
        self.sql_chat = StructuredChat(json_schema=sql_schema)
        self.sql_validator = StructuredChat(json_schema=sql_validation_schema)
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

    def _execute_code_safely(self, code, user_query, context, sql_query, relevant_chunks):
        # Create a local scope for execution with necessary context
        local_vars = {
            "db_path": self.db_path,
            "user_query": user_query,
            "context": context,
            "sql_query": sql_query,
            "relevant_chunks": relevant_chunks,
            "sqlite3": sqlite3,
            "json": json
        }
        
        try:
            exec(code, {}, local_vars)
            if "result" in local_vars:
                return local_vars["result"]
            else:
                return {"error": "Generated code did not produce a 'result' variable"}
        except Exception as e:
            return {"error": f"Error executing generated code: {str(e)}"}

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
        
        # Get the initial SQL query
        initial_sql_query = sql_response["sql_query"]
        
        # Step 2: Generate Python code that executes and formats the SQL query results
        code_prompt = (
            f"Based on the user query '{user_query}' and the generated SQL query below, create Python code that:\n"
            f"1. Executes the SQL query against an SQLite database\n"
            f"2. Formats the results based on the query type (e.g., text answer, table data, or visualization)\n"
            f"3. Returns a structured JSON response\n\n"
            f"SQL Query: {initial_sql_query}\n\n"
            f"Additional Context: {context}\n\n"
            f"Use these variables that will be available in the execution environment:\n"
            f"- db_path: Path to the SQLite database\n"
            f"- user_query: The original user query\n"
            f"- context: Relevant documentation chunks\n"
            f"- sql_query: The SQL query to execute\n"
            f"- relevant_chunks: List of relevant document chunks\n"
            f"- sqlite3: The sqlite3 module\n"
            f"- json: The json module\n\n"
            f"The code should:\n"
            f"- Connect to the database at db_path\n"
            f"- Execute the SQL query\n"
            f"- Analyze the query intent and results to determine the appropriate response format:\n"
            f"  * Text answer for simple facts or explanations\n"
            f"  * Table data (columns and rows) for detailed records\n"
            f"  * Graph data (bar, line, or pie) for trends or comparisons\n"
            f"- Return a JSON object as a variable named 'result' with these fields:\n"
            f"  * 'answer': String with text response (if applicable)\n"
            f"  * 'table_data': Object with 'columns' and 'rows' (if applicable)\n"
            f"  * 'graph_data': Object with 'type', 'labels', 'values', 'title' (if applicable)\n"
            f"  * 'sql_query': The executed SQL query\n"
            f"  * 'query_result': Raw query results\n"
            f"  * 'validation_history': List with the original query\n"
            f"  * 'relevant_chunks': The documentation chunks used\n\n"
            f"Ensure the code is self-contained and handles errors gracefully."
        )
        
        code_response = self.code_generator.send_message(code_prompt)
        
        if not code_response or "python_code" not in code_response:
            return {"error": "Failed to generate Python code", "details": code_response}
        
        generated_code = code_response["python_code"]
        
        # Step 3: Execute the generated Python code
        result = self._execute_code_safely(
            generated_code, 
            user_query, 
            context, 
            initial_sql_query, 
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