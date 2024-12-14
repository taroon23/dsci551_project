from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_cors import CORS
import mysql.connector
import pandas as pd
import re
import os
import random
from sql_func import create_sample_query, input_to_sql


app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

# Global metadata store
metadata_store = {}

# Function to connect to MySQL Database
def connect_to_mysql(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,  
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("Connected to MySQL database successfully!")
            return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

# Function to process CSV and load it into SQL
def process_and_load_csv(csv_path, host, user, password, database):
    try:
        # Extract table name
        table_name = os.path.basename(csv_path).replace('.csv', '').lower()

        # Read CSV into DataFrame
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.replace(' ', '_').str.lower()

        # Extract metadata
        column_names = df.columns
        attributes = []  # Categorical variables
        measures = []    # Continuous variables
        dates = []       # Date variables
        unique_elements = {}

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                dates.append(col)
            elif pd.api.types.is_numeric_dtype(df[col]) and '_id' not in col.lower():
                measures.append(col)
            else:
                attributes.append(col)
                unique_elements[col] = df[col].unique().tolist()

        # Connect to MySQL
        connection = connect_to_mysql(host, user, password, database)
        if connection is None:
            return None, "Failed to connect to the database."

        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_table_query = f"CREATE TABLE {table_name} ("
        for col in df.columns:
            if col in dates:
                create_table_query += f"{col} DATETIME, "
            elif col in measures:
                create_table_query += f"{col} FLOAT, "
            else:
                create_table_query += f"{col} VARCHAR(255), "
        create_table_query = create_table_query.rstrip(', ') + ")"
        cursor.execute(create_table_query)

        # Insert data into table
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        cursor.executemany(insert_query, df.values.tolist())
        connection.commit()

        return {
            "table_name": table_name,
            "column_names": list(column_names),
            "attributes": attributes,
            "measures": measures,
            "dates": dates,
            "unique_elements": unique_elements
        }, None

    except Exception as e:
        return None, str(e)

@app.route('/get-metadata', methods=['GET'])
def get_metadata():
    table_name = request.args.get('table')
    if not table_name:
        return jsonify({"error": "No table name provided."}), 400

    if table_name not in metadata_store:
        return jsonify({"error": f"Metadata for table '{table_name}' not found."}), 404

    return jsonify(metadata_store[table_name])

@app.route('/')
def serve_index():
    return render_template('index.html')

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    file = request.files['file']
    if not file:
        return jsonify({"error": "No file uploaded."}), 400

    file_path = f"./{file.filename}"
    file.save(file_path)

    metadata, error = process_and_load_csv(
        csv_path=file_path,
        host="localhost",
        user="root",
        password="1234",
        database="dsci551_ughh"
    )

    if error:
        return jsonify({"error": error}), 500

    # Save metadata to metadata_store
    metadata_store[metadata["table_name"]] = {
        "column_names": metadata["column_names"],
        "attributes": metadata["attributes"],
        "measures": metadata["measures"],
        "dates": metadata["dates"],
        "unique_elements": metadata["unique_elements"],
    }

    return jsonify({"message": f"Table '{metadata['table_name']}' uploaded successfully.", "metadata": metadata})

@app.route('/preview-table', methods=['GET'])
def preview_table():
    table_name = request.args.get('table')
    if not table_name:
        return jsonify({"error": "No table name provided."}), 400

    connection = connect_to_mysql("localhost", "root", "1234", "dsci551_ughh")
    if connection is None:
        return jsonify({"error": "Failed to connect to the database."}), 500

    try:
        query = f"SELECT * FROM {table_name} LIMIT 10"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        return jsonify(rows)
    except mysql.connector.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    finally:
        connection.close()

@app.route('/process_query', methods=['POST'])
def process_query():
    data = request.json
    input_user_query = data.get('query')
    table_name = data.get('table_name')

    if not table_name or not input_user_query:
        return jsonify({"error": "Missing table name or user input."}), 400

    # Fetch metadata
    metadata = metadata_store.get(table_name)
    if not metadata:
        return jsonify({"error": f"No metadata found for table '{table_name}'."}), 404

    if "sample" in input_user_query.lower():
        # Generate sample queries
        # Detect query type from user input
        query_types = ["random", "group by", "sum", "avg", "min", "max", "where", "order by", "having"]
        detected_query_type = next((qt for qt in query_types if qt in input_user_query.lower()), None)

        if detected_query_type:
            # Generate sample queries based on detected query type
            try:
                queries = create_sample_query(detected_query_type, table_name, metadata_store)
                return jsonify({"samples": queries})
            except ValueError as e:
                return jsonify({"error": str(e)}), 400
        
        else:
            random_query_type =  random.choice(query_types)
            try:
                queries = create_sample_query(random_query_type, table_name, metadata_store)
                return jsonify({"samples": queries})
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

    else:
        # Translate to SQL query
        try:
            translated_query = input_to_sql(input_user_query, table_name, metadata_store)

            # Remove any outer quotes from the translated query
            if translated_query.startswith("'") and translated_query.endswith("'"):
                translated_query = translated_query[1:-1]

            connection = connect_to_mysql("localhost", "root", "1234", "dsci551_ughh")
            if connection is None:
                return jsonify({"error": "Failed to connect to the database."}), 500

            cursor = connection.cursor(dictionary=True)
            cursor.execute(translated_query)
            results = cursor.fetchall()  # Fetch all rows of the query result

            # Close the connection
            connection.close()

            return jsonify({
                "translated_query": translated_query,
                "data": results
            })
        except Exception as e:
            return jsonify({"error": f"Failed to execute query: {str(e)}"}), 500

########################################################################################################################################################################
# NO SQL PART

import nosql_func
from nosql_func import generate_random_mongodb_query , parse_conditions, parse_sorting, parse_display_columns, parse_aggregation

# Global metadata store
nosql_metadata_store = {}

# MongoDB Connection
def connect_to_mongodb_localhost(database_name):
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client[database_name]
        print(f"Connected to MongoDB database: {database_name}")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

import pandas as pd
from pymongo import MongoClient

def csv_to_mongo_loader(file_path, mongo_uri, db_name, collection_name):
    # Initialize result dictionary
    result = {}

    try:
        # Step 1: Read the CSV file into a DataFrame
        data_frame = pd.read_csv(file_path)
        data_frame.columns = data_frame.columns.str.replace(' ', '_').str.lower()
        columns = data_frame.columns.tolist()

        # Initialize lists for attributes and measures
        categorical_columns = []
        numeric_columns = []
        unique_values = {}

        # Classify columns based on their data types
        for column in data_frame.columns:
            if pd.api.types.is_datetime64_any_dtype(data_frame[column]):
                categorical_columns.append(column)
                unique_values[column] = data_frame[column].dropna().unique().tolist()
            elif pd.api.types.is_numeric_dtype(data_frame[column]) and not column.endswith("id"):
                numeric_columns.append(column)
            else:
                categorical_columns.append(column)
                unique_values[column] = data_frame[column].dropna().unique().tolist()

        print(f"Successfully loaded CSV with {len(data_frame)} rows and {len(data_frame.columns)} columns.")

        # Step 2: Connect to MongoDB and insert data
        client = MongoClient(mongo_uri)
        database = client[db_name]
        collection = database[collection_name]

        # Convert the DataFrame to a list of dictionaries
        records = data_frame.to_dict(orient="records")
        collection.insert_many(records)

        print(f"Data inserted into MongoDB collection '{collection_name}' in database '{db_name}'.")

        # Save metadata to the result dictionary using the collection name as the key
        result[collection_name] = {
            "columns": columns,
            "categorical_columns": categorical_columns,
            "numeric_columns": numeric_columns,
            "unique_values": unique_values,
        }

    except Exception as err:
        print(f"Error during CSV to MongoDB loading: {err}")
        return None

    return result

# Upload CSV or JSON file for MongoDB
@app.route('/upload-nosql', methods=['POST'])
def upload_nosql():
    file = request.files['file']
    if not file:
        return jsonify({"error": "No file uploaded."}), 400

    # Save uploaded file
    file_path = f"./{file.filename}"
    file.save(file_path)

    # Determine file type (CSV or JSON)
    file_type = "csv" if file.filename.endswith(".csv") else "json"
    collection_name = file.filename.replace(f".{file_type}", "").lower()

    try:
        # Load data into MongoDB
        if file_type == "csv":
            metadata = csv_to_mongo_loader(
                file_path=file_path,
                mongo_uri="mongodb://localhost:27017/",
                db_name="nosql_db",
                collection_name=collection_name,
            )
        else:
            # JSON handling can be implemented similarly
            return jsonify({"error": "JSON upload not yet implemented."}), 400

        # Save metadata to global store
        nosql_metadata_store[collection_name] = metadata[collection_name]

        return jsonify({
            "message": f"Collection '{collection_name}' uploaded successfully.",
            "metadata": metadata
        })
    except Exception as e:
        return jsonify({"error": f"Failed to upload file: {str(e)}"}), 500

@app.route('/preview-nosql', methods=['GET'])
def preview_nosql():
    collection_name = request.args.get("collection")
    if not collection_name:
        return jsonify({"error": "No collection name provided."}), 400

    # Connect to MongoDB and fetch preview data
    db = connect_to_mongodb_localhost("nosql_db")
    if db is None:
        return jsonify({"error": "Failed to connect to MongoDB."}), 500

    try:
        collection = db[collection_name]
        documents = list(collection.find().limit(10))  # Preview 10 documents
        for doc in documents:
            doc["_id"] = str(doc["_id"])  # Convert ObjectId to string for JSON compatibility
        return jsonify(documents)
    except Exception as e:
        return jsonify({"error": f"Error fetching collection preview: {str(e)}"}), 500


@app.route('/process-nosql-query', methods=['POST'])
def process_nosql_query():
    print("Received a request to /process-nosql-query")
    data = request.json
    print(f"Request data: {data}")
    
    user_query = data.get("query")
    collection_name = data.get("collection")
    
    if not user_query or not collection_name:
        print("Error: Missing query or collection name.")
        return jsonify({"error": "Missing query or collection name."}), 400
    
    # Log metadata
    print(f"Selected collection: {collection_name}")
    metadata = nosql_metadata_store.get(collection_name)
    if not metadata:
        print(f"Error: No metadata found for collection '{collection_name}'.")
        return jsonify({"error": f"No metadata found for collection '{collection_name}'."}), 404

    if "sample" in user_query.lower():
        try:
            nosql_query_types = [
                'eq', 'ne', 'gt', 'lt', 'gte', 'lte',
                'and', 'or', 'nor', 'in', 'nin', 'all',
                'elemmatch', 'match', 'sort', 'group'
            ]
            # Split user query into words and match exactly
            user_words = user_query.lower().split()
            detected_nosql_query_type = next((qt for qt in nosql_query_types if qt in user_words), None)
            
            if not detected_nosql_query_type:
                raise ValueError("No valid query type detected in the user query.")
            
            query = generate_random_mongodb_query(detected_nosql_query_type, collection_name, nosql_metadata_store)
            print(f"Generated sample query: {query}")
            return jsonify({"sample_query": query})

        except Exception as e:
            print(f"Error generating sample query: {str(e)}")
            return jsonify({"error": f"Error generating sample query: {str(e)}"}), 500
    
    try:
        # Parse query components
        aggregation = parse_aggregation(user_query, collection_name, nosql_metadata_store)
        display_columns = parse_display_columns(user_query, collection_name, nosql_metadata_store)
        conditions = parse_conditions(user_query, collection_name, nosql_metadata_store)
        sorting = parse_sorting(user_query, collection_name, nosql_metadata_store)

        print(f"Aggregation: {aggregation}")
        print(f"Conditions: {conditions}")
        print(f"Display Columns: {display_columns}")
        print(f"Sorting: {sorting}")

        if aggregation:
            # Aggregation query
            pipeline = []
            if conditions:
                pipeline.append({"$match": conditions})
            pipeline.append(aggregation)
            if sorting:
                pipeline.append({"$sort": sorting})
            query_string = f"db.{collection_name}.aggregate({str(pipeline).replace('None', 'null')})"

            # Execute aggregation query
            db = connect_to_mongodb_localhost("nosql_db")
            collection = db[collection_name]
            results = list(collection.aggregate(pipeline))
        else:
            # Standard find query
            query_string = f"db.{collection_name}.find("
            query_string += f"{conditions}, {display_columns})" if display_columns else f"{conditions}, {{}})"
            if sorting:
                query_string += f".sort({sorting})"

            # Execute find query
            db = connect_to_mongodb_localhost("nosql_db")
            collection = db[collection_name]
            cursor = collection.find(conditions, display_columns)
            if sorting:
                cursor = cursor.sort(list(sorting.items()))
            results = list(cursor.limit(10))  # Limit results for safety

        print(f"Query String: {query_string}")
        print(f"Query Results: {results}")

        # Convert ObjectId to string for JSON response
        for result in results:
            if "_id" in result:
                result["_id"] = str(result["_id"])
        return jsonify({"query": query_string, "data": results})
    except Exception as e:
        print(f"Error processing query: {str(e)}")
        return jsonify({"error": f"Failed to process query: {str(e)}"}), 500
    

if __name__ == '__main__':
    app.run(debug=True)
