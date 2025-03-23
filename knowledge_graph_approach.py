import sqlite3
import pandas as pd
from neo4j import GraphDatabase
from pollinations import StructuredChat
import json

# Configuration
DB_PATH = "my_database.db"
TABLE_NAME = "sample_sales_data"
NEO4J_URI = "bolt://localhost:7687"  # Default Neo4j URI
NEO4J_USER = "admin"
NEO4J_PASSWORD = "mvemjsun"  

# JSON schema for Cypher query generation
cypher_schema = {
    "type": "object",
    "properties": {
        "cypher_query": {"type": "string"}
    },
    "required": ["cypher_query"]
}

# JSON schema for final response
response_schema = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "cypher_query": {"type": "string"},
        "graph_data": {
            "type": "array",
            "items": {"type": "object"}
        }
    },
    "required": ["answer", "cypher_query", "graph_data"]
}

class Neo4jKnowledgeGraphSystem:
    def __init__(self, db_path, table_name, neo4j_uri, neo4j_user, neo4j_password):
        self.db_path = db_path
        self.table_name = table_name
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.cypher_chat = StructuredChat(json_schema=cypher_schema)
        self.response_chat = StructuredChat(json_schema=response_schema)
        
        # Initialize knowledge graph
        self._build_knowledge_graph()

    def _fetch_table_data(self):
        """Fetch all data from the SQLite table."""
        conn = sqlite3.connect(self.db_path)
        query = f"SELECT * FROM {self.table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def _build_knowledge_graph(self):
        """Build a knowledge graph in Neo4j from table data."""
        df = self._fetch_table_data()
        
        with self.driver.session() as session:
            # Clear existing graph (for fresh start)
            session.run("MATCH (n) DETACH DELETE n")
            
            for _, row in df.iterrows():
                order_id = row["Order_ID"]
                product = row["Product"]
                region = row["Region"]
                
                # Create nodes with properties
                session.run(
                    "MERGE (o:Order {order_id: $order_id}) "
                    "SET o += $attributes",
                    order_id=order_id, attributes=row.to_dict()
                )
                session.run(
                    "MERGE (p:Product {name: $name})",
                    name=product
                )
                session.run(
                    "MERGE (r:Region {name: $name})",
                    name=region
                )
                
                # Create relationships
                session.run(
                    "MATCH (o:Order {order_id: $order_id}), (p:Product {name: $product}) "
                    "MERGE (o)-[:HAS_PRODUCT]->(p)",
                    order_id=order_id, product=product
                )
                session.run(
                    "MATCH (o:Order {order_id: $order_id}), (r:Region {name: $region}) "
                    "MERGE (o)-[:SOLD_IN]->(r)",
                    order_id=order_id, region=region
                )
        
        print(f"Built Neo4j knowledge graph from {self.table_name}")

    def _execute_cypher(self, cypher_query):
        """Execute the Cypher query and return results."""
        try:
            with self.driver.session() as session:
                result = session.run(cypher_query)
                return [dict(record) for record in result]
        except Exception as e:
            return {"error": str(e)}

    def query(self, user_query):
        """Convert user query to Cypher, execute it, and generate structured response."""
        # Step 1: Convert user query to Cypher
        cypher_prompt = (
            f"Given a Neo4j knowledge graph with the following structure:\n"
            f"- Nodes:\n"
            f"  - Order (properties: order_id, Product, Region, Total_Amount, etc.)\n"
            f"  - Product (property: name)\n"
            f"  - Region (property: name)\n"
            f"- Relationships:\n"
            f"  - Order -[:HAS_PRODUCT]-> Product\n"
            f"  - Order -[:SOLD_IN]-> Region\n"
            f"Convert this natural language query into a valid Cypher query:\n"
            f"'{user_query}'\n"
            f"Return only the Cypher query in the JSON response."
        )
        cypher_response = self.cypher_chat.send_message(cypher_prompt)
        
        if not cypher_response or "cypher_query" not in cypher_response:
            return {"error": "Failed to generate Cypher query", "details": cypher_response}
        
        cypher_query = cypher_response["cypher_query"]
        
        # Step 2: Execute the Cypher query
        graph_data = self._execute_cypher(cypher_query)
        
        if isinstance(graph_data, dict) and "error" in graph_data:
            return {"error": "Cypher execution failed", "details": graph_data["error"], "cypher_query": cypher_query}
        
        # Step 3: Generate final response
        response_prompt = (
            f"Based on the following Cypher query and its results from the {self.table_name} knowledge graph:\n"
            f"Cypher Query: {cypher_query}\n"
            f"Results: {json.dumps(graph_data, indent=2)}\n\n"
            f"Provide an answer to the user's original question: '{user_query}'"
        )
        final_response = self.response_chat.send_message(response_prompt)
        
        return final_response

    def __del__(self):
        """Close the Neo4j driver when the object is destroyed."""
        self.driver.close()

def main():
    # Initialize Neo4jKnowledgeGraphSystem
    kg = Neo4jKnowledgeGraphSystem(DB_PATH, TABLE_NAME, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    # Example queries
    queries = [
        "What’s the average total amount for Mouse?",
        "Which product has the highest total amount?",
        "Summarize sales in the North region."
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        response = kg.query(query)
        print("Response:", json.dumps(response, indent=2))

if __name__ == "__main__":
    main()