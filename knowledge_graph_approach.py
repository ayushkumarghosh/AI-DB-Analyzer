import sqlite3
import pandas as pd
import networkx as nx
from pollinations import StructuredChat
import json

# Configuration
DB_PATH = "my_database.db"
TABLE_NAME = "sample_sales_data"

# JSON schema for final response
response_schema = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "graph_data": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "entity": {"type": "string"},
                    "relationship": {"type": "string"},
                    "related_entity": {"type": "string"},
                    "attributes": {"type": "object"}
                }
            }
        }
    },
    "required": ["answer", "graph_data"]
}

class KnowledgeGraphSystem:
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name
        self.graph = nx.DiGraph()  # Directed graph for relationships
        self.chat = StructuredChat(json_schema=response_schema)
        
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
        """Build a knowledge graph from table data."""
        df = self._fetch_table_data()
        
        for _, row in df.iterrows():
            order_id = row["Order_ID"]
            product = row["Product"]
            region = row["Region"]
            
            # Add nodes
            self.graph.add_node(order_id, type="Order", attributes=row.to_dict())
            self.graph.add_node(product, type="Product")
            self.graph.add_node(region, type="Region")
            
            # Add relationships
            self.graph.add_edge(order_id, product, relationship="has_product")
            self.graph.add_edge(order_id, region, relationship="sold_in")
            self.graph.add_edge(product, order_id, relationship="ordered_in")
            self.graph.add_edge(region, order_id, relationship="contains_order")
        
        print(f"Built knowledge graph with {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges")

    def _extract_relevant_subgraph(self, query):
        """Extract relevant information from the graph based on the query."""
        # Simple keyword-based extraction (can be enhanced with NLP)
        query_lower = query.lower()
        relevant_data = []
        
        for node in self.graph.nodes():
            node_data = self.graph.nodes[node]
            node_type = node_data.get("type", "")
            
            # Check if node matches query keywords
            if (node.lower() in query_lower or 
                (node_type == "Order" and any(col.lower() in query_lower for col in node_data.get("attributes", {})))):
                # Get neighbors and relationships
                for neighbor in self.graph.neighbors(node):
                    edge_data = self.graph.get_edge_data(node, neighbor)
                    attrs = self.graph.nodes[neighbor].get("attributes", {})
                    if node_type == "Order":
                        attrs = node_data["attributes"]
                    relevant_data.append({
                        "entity": node,
                        "relationship": edge_data["relationship"],
                        "related_entity": neighbor,
                        "attributes": attrs
                    })
        
        return relevant_data[:5]  # Limit to 5 entries for context brevity

    def query(self, user_query):
        """Use knowledge graph to answer the user query."""
        # Extract relevant subgraph data
        graph_data = self._extract_relevant_subgraph(user_query)
        
        if not graph_data:
            graph_data = [{"entity": "N/A", "relationship": "none", "related_entity": "N/A", "attributes": {}}]
        
        # Construct prompt for StructuredChat
        prompt = (
            f"Based on the following knowledge graph data from the {self.table_name} table:\n"
            f"{json.dumps(graph_data, indent=2)}\n\n"
            f"Provide an answer to this question: '{user_query}'"
        )
        
        # Get structured response
        response = self.chat.send_message(prompt)
        return response

def main():
    # Initialize KnowledgeGraphSystem
    kg = KnowledgeGraphSystem(DB_PATH, TABLE_NAME)
    
    # Example queries
    queries = [
        "What’s the average total amount for mouse?",
        "Which product has the highest total amount?",
        "Summarize sales in the North region."
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        response = kg.query(query)
        print("Response:", json.dumps(response, indent=2))

if __name__ == "__main__":
    main()