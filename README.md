# AI Database Analyzer

An interactive Streamlit application that allows you to query your database using natural language.

## Features

- Natural language query translation to SQL
- Interactive Streamlit UI
- Multiple output formats:
  - Text answers
  - Data tables
  - Visualizations (bar charts, line charts, pie charts)
- Contextual retrieval using ChromaDB
- Example queries to get started

## Installation

1. Clone this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Make sure you have the necessary database files:
   - `my_database.db` - SQLite database with sample data
   - `chroma_db` - Vector database for contextual retrieval

## Usage

1. Run the Streamlit application:

```bash
streamlit run app.py
```

2. Enter your natural language query in the text area
3. Click "Analyze" to process your query
4. View the results in the appropriate format (text, table, or visualization)

## Example Queries

- What are the orders with the highest and lowest total amounts after applying the highest discount?
- Show all pending orders in the North region where shipping cost exceeds 30 dollars.
- What's the average unit price for each product category, excluding orders with zero discount?
- Which completed orders from the last 30 days have a total amount greater than 1000 dollars?
- Find orders where the discount reduces the total unit price cost to less than the shipping cost.

## Architecture

The application consists of:

1. `text_to_query.py` - Core logic for converting text to SQL queries
2. `app.py` - Streamlit UI
3. SQLite database (`my_database.db`)
4. ChromaDB vector database for context retrieval

## Requirements

- Python 3.8+
- Streamlit
- Pandas
- Plotly
- ChromaDB
- Pollinations library