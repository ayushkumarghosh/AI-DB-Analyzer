# AI-DB-Analyzer

AI-DB-Analyzer is a Python project designed to:
- Upload CSV data into a SQLite database.
- Create a vector database using documentation files.
- Generate and validate SQL queries based on natural language input.
- Return structured responses including query results, explanations, and visual data suggestions.

## Project Components

### Data Upload
- **File:** [data_upload.py](data_upload.py)
- **Functionality:** 
  - Loads CSV files from the `data/` folder into the SQLite database (`my_database.db`).
  - Normalizes CSV column names (replacing spaces, dots, and dashes).
  - Loads documentation files (`documentation.txt` and `db_schema.sql`) into a Chroma vector database for later reference using simple chunking.

### Text to SQL Query
- **File:** [text_to_query.py](text_to_query.py)
- **Functionality:**
  - Uses natural language queries to generate corresponding SQL queries.
  - Incorporates context retrieved from the vector database (created via documentation) to better understand user queries.
  - Validates and corrects generated SQL queries in a loop until they execute correctly on the SQLite database.
  - Provides a structured response (text answer, table data, graph data) by leveraging a structured chat model.

### Structured Chat & Query Generation
- **File:** [pollinations.py](pollinations.py)
- **Functionality:**
  - Contains the `StructuredChat` class that integrates with an OpenAI endpoint to generate JSON responses.
  - Main utility for converting natural language prompts into SQL queries and for generating structured response messages.

## Setup & Installation

1. **Clone the Repository**

2. **Install Requirements**
    ```pip install -r requirements.txt```

3. **Generate fake data**
    ```python generate_fake_data.py```

4. **Run Data Upload**
    ```python data_upload.py```

5. **Generate Query Output**
    ```python text_to_query.py```