# ChatDB: Interactive Database Querying Application

## Project Overview
ChatDB is an interactive web application designed to help users learn and execute database queries using both SQL and NoSQL systems. Users can upload datasets, query them using natural language or database syntax, and view the results in an intuitive user interface. The application supports MySQL for SQL operations and MongoDB for NoSQL operations.

---

## Features
- **Dataset Upload**: Users can upload CSV files (for SQL) or JSON files (for NoSQL) to populate the database.
- **Database Interaction**: 
  - SQL (MySQL): Supports advanced query constructs such as `GROUP BY`, `HAVING`, and more.
  - NoSQL (MongoDB): Enables dynamic aggregation pipelines and query generation.
- **Natural Language Querying**: Converts user-friendly natural language inputs into SQL/NoSQL queries.
- **Dynamic Results**: Displays query results in a user-friendly table format.
- **Sample Query Suggestions**: Generates sample queries based on the dataset's structure.

---

## Project Structure
```
project-directory/
|-- app.py                 # Main Flask application
|-- sql_func.py            # SQL query generation logic
|-- nosql_func.py          # NoSQL query generation logic
|-- static/                # Static files (JavaScript, CSS)
|   |-- script.js          # JavaScript for frontend interactions
|   |-- style.css          # Stylesheet for the frontend
|-- templates/             # HTML templates
|   |-- index.html         # Main UI of the application
```

---

## How to Run the Application

1. **Install Dependencies**:
   Ensure you have Python installed. Install the required libraries.

2. **Set Up Databases**:
   - Install and configure **MySQL** for SQL operations.
   - Install and configure **MongoDB** for NoSQL operations.

3. **Run the Flask Application**:
   Execute the following command in the project directory:
   ```bash
   python app.py
   ```

4. **Access the Application**:
   Open a web browser and go to:
   ```
   http://127.0.0.1:5000/
   ```

---

## Functionalities

### 1. **Dataset Upload**:
   - Upload a CSV file for MySQL or a JSON file for MongoDB.
   - The application stores the uploaded data in the selected database.

### 2. **Database Interaction**:
   - Choose between SQL or NoSQL databases.
   - Select an existing table/collection or upload a new dataset.
   - View a sample of the table/collection.

### 3. **Query Input**:
   - Enter natural language queries or database-specific queries.
   - Generate sample queries dynamically.

### 4. **Result Display**:
   - See the generated SQL/NoSQL query and its results in a tabular format.
