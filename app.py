import streamlit as st
import json
import pandas as pd
import plotly.express as px
from text_to_query import TextToQuery, DB_PATH, TABLE_NAME

# Page configuration
st.set_page_config(
    page_title="AI Database Analyzer",
    page_icon="📊",
    layout="wide"
)

# Initialize TextToQuery
@st.cache_resource
def get_text_to_query():
    return TextToQuery(DB_PATH, TABLE_NAME)

t2q = get_text_to_query()

# Initialize session state for chat history if it doesn't exist
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Header
st.title("AI Database Analyzer")
st.markdown("Ask questions about your database in natural language")

# Chat container for displaying conversation history
chat_container = st.container()

# Function to add user message to chat history
def add_user_message(message):
    st.session_state.chat_history.append({"role": "user", "content": message})

# Function to add assistant message to chat history
def add_assistant_message(message, data=None):
    st.session_state.chat_history.append({
        "role": "assistant", 
        "content": message,
        "data": data
    })

# Display chat history
with chat_container:
    for message in st.session_state.chat_history:
        if message["role"] == "user":
            st.chat_message("user").write(message["content"])
        else:
            with st.chat_message("assistant"):
                st.write(message["content"])
                
                # Display data if available
                data = message.get("data", {})
                
                # Display the SQL query used
                if data and "sql_query" in data:
                    with st.expander("SQL Query", expanded=False):
                        st.code(data["sql_query"], language="sql")
                
                # Display the relevant context if available
                if data and "relevant_chunks" in data and data["relevant_chunks"]:
                    with st.expander("Relevant Context", expanded=False):
                        for chunk in data["relevant_chunks"]:
                            st.markdown(f"- {chunk}")
                
                # Display table data if available
                if data and "table_data" in data and data["table_data"]:
                    table_data = data["table_data"]
                    if "columns" in table_data and "rows" in table_data:
                        df = pd.DataFrame(table_data["rows"], columns=table_data["columns"])
                        st.dataframe(df, use_container_width=True)
                
                # Display graph if available
                if data and "graph_data" in data and data["graph_data"]:
                    graph_data = data["graph_data"]
                    if "type" in graph_data and "labels" in graph_data and "values" in graph_data:
                        chart_data = pd.DataFrame({
                            "labels": graph_data["labels"],
                            "values": graph_data["values"]
                        })
                        
                        title = graph_data.get("title", "")
                        
                        if graph_data["type"] == "bar":
                            fig = px.bar(chart_data, x="labels", y="values", title=title)
                            st.plotly_chart(fig, use_container_width=True)
                        elif graph_data["type"] == "line":
                            fig = px.line(chart_data, x="labels", y="values", title=title)
                            st.plotly_chart(fig, use_container_width=True)
                        elif graph_data["type"] == "pie":
                            fig = px.pie(chart_data, names="labels", values="values", title=title)
                            st.plotly_chart(fig, use_container_width=True)
                            
                # Display raw results if no other data is available
                if (data and not data.get("table_data") and 
                    not data.get("graph_data") and 
                    "query_result" in data):
                    with st.expander("Raw Results", expanded=False):
                        st.json(data["query_result"])

# Input area
query = st.chat_input("Ask a question about your database...")

# Process user input
if query:
    # Add user message to chat history
    add_user_message(query)
    
    # Display user message immediately
    st.chat_message("user").write(query)
    
    # Show assistant is thinking
    with st.chat_message("assistant"):
        with st.spinner("Analyzing your query..."):
            try:
                # Get response from TextToQuery
                response = t2q.query(query)
                
                # Prepare assistant message
                answer_text = response.get("answer", "")
                if not answer_text:
                    if "table_data" in response:
                        answer_text = "Here are the results of your query:"
                    elif "graph_data" in response:
                        answer_text = f"Here's a {response['graph_data']['type']} chart based on your query:"
                    else:
                        answer_text = "I've analyzed your query and found the following:"
                
                # Add assistant message to chat history with data
                add_assistant_message(answer_text, response)
                
                # Force a rerun to update the chat container
                st.rerun()
                
            except Exception as e:
                error_msg = f"Error processing query: {str(e)}"
                add_assistant_message(error_msg)
                st.error(error_msg)

# Sidebar with example queries
with st.sidebar:
    st.header("Example Queries")
    example_queries = [
        "What are the orders with the highest and lowest total amounts after applying the highest discount?",
        "Show all pending orders in the North region where shipping cost exceeds 30 dollars.",
        "What's the average unit price for each product category, excluding orders with zero discount?",
        "Which completed orders from the last 30 days have a total amount greater than 1000 dollars?",
        "Find orders where the discount reduces the total unit price cost to less than the shipping cost."
    ]
    
    st.markdown("Click on an example to try it:")
    for example in example_queries:
        if st.button(example, key=f"example_{example_queries.index(example)}"):
            st.session_state.example_query = example
            st.rerun()  # Updated from experimental_rerun
    
    # Handle running the selected example
    if "example_query" in st.session_state:
        query = st.session_state.example_query
        # Add user message to chat history
        add_user_message(query)
        
        # Show assistant is thinking
        with st.spinner("Analyzing your query..."):
            try:
                # Get response from TextToQuery
                response = t2q.query(query)
                
                # Prepare assistant message
                answer_text = response.get("answer", "")
                if not answer_text:
                    if "table_data" in response:
                        answer_text = "Here are the results of your query:"
                    elif "graph_data" in response:
                        answer_text = f"Here's a {response['graph_data']['type']} chart based on your query:"
                    else:
                        answer_text = "I've analyzed your query and found the following:"
                
                # Add assistant message to chat history with data
                add_assistant_message(answer_text, response)
                
            except Exception as e:
                error_msg = f"Error processing query: {str(e)}"
                add_assistant_message(error_msg)
        
        # Clear the example query
        del st.session_state.example_query
        
        # Force a rerun to update the chat container
        st.rerun()
        
    # Add a button to clear chat history
    if st.button("Clear Chat History"):
        st.session_state.chat_history = []
        st.rerun()

# Footer
st.markdown("---")
st.markdown("AI Database Analyzer - Powered by ChromaDB and LLMs") 