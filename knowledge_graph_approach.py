import sqlite3
import pandas as pd
from neo4j import GraphDatabase
from pollinations import StructuredChat
import json

# Configuration
DB_PATH = "my_database.db"
TABLE_NAME = "sample_sales_data"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "admin"
NEO4J_PASSWORD = "mvemjsun"  # Replace with your Neo4j password

# JSON schema for graph structure inference
graph_schema = {
    "type": "object",
    "properties": {
        "entities": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "column": {"type": "string"},
                    "attributes": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["name", "column"]
            }
        },
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "from_entity": {"type": "string"},
                    "to_entity": {"type": "string"},
                    "relationship_name": {"type": "string"}
                },
                "required": ["from_entity", "to_entity", "relationship_name"]
            }
        }
    },
    "required": ["entities", "relationships"]
}

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

class DynamicNeo4jKnowledgeGraphSystem:
    def __init__(self, db_path, table_name, neo4j_uri, neo4j_user, neo4j_password):
        self.db_path = db_path
        self.table_name = table_name
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.graph_chat = StructuredChat(json_schema=graph_schema)
        self.cypher_chat = StructuredChat(json_schema=cypher_schema)
        self.response_chat = StructuredChat(json_schema=response_schema)
        
        # Initialize knowledge graph dynamically
        self.graph_structure = self._infer_graph_structure()
        self._build_knowledge_graph()

    def _fetch_table_data(self, sample_size=5):
        """Fetch table data, optionally with a sample size."""
        conn = sqlite3.connect(self.db_path)
        query = f"SELECT * FROM {self.table_name} LIMIT {sample_size}"
        df = pd.read_sql_query(query, conn)
        full_df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", conn)
        conn.close()
        return df, full_df

    def _get_table_schema(self):
        """Fetch the schema of the table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.table_name})")
        columns = cursor.fetchall()
        conn.close()
        return [{"name": col[1], "type": col[2]} for col in columns]

    def _infer_graph_structure(self):
        """Use StructuredChat to dynamically infer entities and relationships."""
        sample_df, _ = self._fetch_table_data()
        schema = self._get_table_schema()
        
        schema_desc = "Table Schema:\n" + "\n".join([f"- {col['name']} ({col['type']})" for col in schema])
        sample_data = sample_df.to_dict(orient="records")
        
        prompt = (
            f"Given the following table schema and sample data from '{self.table_name}':\n"
            f"Schema:\n{schema_desc}\n\n"
            f"Sample Data:\n{json.dumps(sample_data, indent=2)}\n\n"
            f"Identify the entities (nodes) and relationships (edges) to build a knowledge graph.\n"
            f"For each entity, specify its name, primary column, and optional attribute columns.\n"
            f"For each relationship, specify the from_entity, to_entity, and relationship_name.\n"
            f"Return the structure in the required JSON format."
        )
        
        structure = self.graph_chat.send_message(prompt)
        if not structure or "entities" not in structure or "relationships" not in structure:
            raise ValueError("Failed to infer graph structure", structure)
        
        print("Inferred Graph Structure:", json.dumps(structure, indent=2))
        return structure

    def _build_knowledge_graph(self):
        """Build a dynamic knowledge graph in Neo4j based on inferred structure."""
        _, full_df = self._fetch_table_data()
        
        with self.driver.session() as session:
            # Clear existing graph
            session.run("MATCH (n) DETACH DELETE n")
            
            # Create nodes
            for entity in self.graph_structure["entities"]:
                entity_name = entity["name"]
                column = entity["column"]
                attributes = entity.get("attributes", [])
                
                for _, row in full_df.iterrows():
                    node_props = {col: row[col] for col in attributes if col in row}
                    node_props["key"] = row[column]  # Unique identifier
                    session.run(
                        f"MERGE (n:{entity_name} {{key: $key}}) "
                        f"SET n += $props",
                        key=row[column], props=node_props
                    )
            
            # Create relationships
            for rel in self.graph_structure["relationships"]:
                from_entity = rel["from_entity"]
                to_entity = rel["to_entity"]
                rel_name = rel["relationship_name"]
                
                for _, row in full_df.iterrows():
                    from_key = row[next(e["column"] for e in self.graph_structure["entities"] if e["name"] == from_entity)]
                    to_key = row[next(e["column"] for e in self.graph_structure["entities"] if e["name"] == to_entity)]
                    session.run(
                        f"MATCH (a:{from_entity} {{key: $from_key}}), (b:{to_entity} {{key: $to_key}}) "
                        f"MERGE (a)-[r:{rel_name}]->(b)",
                        from_key=from_key, to_key=to_key
                    )
        
        print(f"Built dynamic Neo4j knowledge graph from {self.table_name}")

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
            f"Entities:\n{json.dumps(self.graph_structure['entities'], indent=2)}\n"
            f"Relationships:\n{json.dumps(self.graph_structure['relationships'], indent=2)}\n"
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
        self.driver.close()

def main():
    # Initialize DynamicNeo4jKnowledgeGraphSystem
    kg = DynamicNeo4jKnowledgeGraphSystem(DB_PATH, TABLE_NAME, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
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