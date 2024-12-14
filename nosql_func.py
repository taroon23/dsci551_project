import random
from pymongo import MongoClient

def generate_random_mongodb_query(query_type, collection_name, collection_metadata):
    # Ensure the collection exists in the metadata store
    if collection_name not in collection_metadata:
        raise ValueError(f"Collection '{collection_name}' not found in metadata store.")

    metadata = collection_metadata[collection_name]
    attributes = metadata.get("categorical_columns", [])
    numeric_columns = metadata.get("numeric_columns", [])
    unique_values = metadata.get("unique_values", {})

    # Define query generators
    def generate_filter_queries(operator):
        """Generate queries with filter conditions."""
        queries = []
        for _ in range(5):
            if operator in ['eq', 'ne', 'gt', 'lt', 'gte', 'lte']:
                column = random.choice(numeric_columns if numeric_columns else attributes)
                value = random.randint(1, 100) if column in numeric_columns else random.choice(unique_values.get(column, ["unknown"]))
                query = {column: {f"${operator}": value}}
                print(f"Generated Query ({operator}): {query}")
                queries.append(query)
        return queries

    def generate_sorting_queries():
        """Generate queries with sorting clauses."""
        queries = []
        for _ in range(5):
            column = random.choice(attributes + numeric_columns)
            sort_order = random.choice([1, -1])  # 1 for ascending, -1 for descending
            query = {"$sort": {column: sort_order}}
            print(f"Generated Query (Sort): {query}")
            queries.append(query)
        return queries

    def generate_group_queries():
        """Generate queries with grouping and aggregation."""
        queries = []
        for _ in range(5):
            group_column = random.choice(attributes)
            agg_column = random.choice(numeric_columns)
            query = {
                "$group": {
                    "_id": f"${group_column}",
                    "total": {"$sum": f"${agg_column}"},
                }
            }
            print(f"Generated Query (Group): {query}")
            queries.append(query)
        return queries

    def generate_compound_queries(operator):
        """Generate compound queries using logical operators."""
        queries = []
        for _ in range(5):
            conditions = []
            for _ in range(2):  # Create two conditions for compound queries
                column = random.choice(attributes + numeric_columns)
                value = random.randint(1, 100) if column in numeric_columns else random.choice(unique_values.get(column, ["unknown"]))
                conditions.append({column: {"$eq": value}})
            query = {f"${operator}": conditions}
            print(f"Generated Query ({operator}): {query}")
            queries.append(query)
        return queries

    def generate_in_queries(operator):
        """Generate queries using 'in' or 'nin' operators."""
        queries = []
        for _ in range(5):
            column = random.choice(attributes)
            values = random.sample(unique_values.get(column, ["unknown"]), min(3, len(unique_values.get(column, []))))
            query = {column: {f"${operator}": values}}
            print(f"Generated Query ({operator}): {query}")
            queries.append(query)
        return queries

    def generate_all_queries():
        """Generate queries using the '$all' operator."""
        queries = []
        for _ in range(5):
            column = random.choice(attributes)
            values = random.sample(unique_values.get(column, ["unknown"]), min(3, len(unique_values.get(column, []))))
            query = {column: {"$all": values}}
            print(f"Generated Query (All): {query}")
            queries.append(query)
        return queries

    def generate_elem_match_queries():
        """Generate queries using the '$elemMatch' operator."""
        queries = []
        for _ in range(5):
            column = random.choice(attributes)
            value = random.choice(unique_values.get(column, ["unknown"]))
            query = {column: {"$elemMatch": {"$eq": value}}}
            print(f"Generated Query (ElemMatch): {query}")
            queries.append(query)
        return queries

    def generate_match_queries():
        """Generate queries using the '$match' operator."""
        queries = []
        for _ in range(5):
            column = random.choice(attributes + numeric_columns)
            value = random.randint(1, 100) if column in numeric_columns else random.choice(unique_values.get(column, ["unknown"]))
            query = {"$match": {column: value}}
            print(f"Generated Query (Match): {query}")
            queries.append(query)
        return queries

    # Map query types to their respective generator functions
    query_handlers = {
        operator: lambda op=operator: generate_filter_queries(op) for operator in ['eq', 'ne', 'gt', 'lt', 'gte', 'lte']
    }

    query_handlers.update({
        "and": lambda: generate_compound_queries("and"),
        "or": lambda: generate_compound_queries("or"),
        "nor": lambda: generate_compound_queries("nor"),
        "in": lambda: generate_in_queries("in"),
        "nin": lambda: generate_in_queries("nin"),
        "all": generate_all_queries,
        "elemmatch": generate_elem_match_queries,
        "match": generate_match_queries,
        "sort": generate_sorting_queries,
        "group": generate_group_queries,
        "group by": generate_group_queries,
    })

    # Normalize the query type and ensure it's valid
    normalized_query_type = query_type.lower()
    if normalized_query_type not in query_handlers:
        raise ValueError(f"Unsupported query type: {query_type}")

    # Generate and return queries
    return query_handlers[normalized_query_type]()


import re

def parse_conditions(user_query, collection_name, collection_metadata):
    metadata = collection_metadata[collection_name]
    attributes = metadata.get("categorical_columns", [])
    numeric_columns = metadata.get("numeric_columns", [])
    column_names = attributes + numeric_columns

    patterns = {
        "greater_than": r"(\w+)\s+(?:is|are)?\s*(?:greater than|above|more than)\s*(\d+)",
        "less_than": r"(\w+)\s+(?:is|are)?\s*(?:less than|below|under)\s*(\d+)",
        "equals": r"(\w+)\s+(?:is|are)?\s*(?:equal to|equals|matching)\s*(\w+)",
        "not_equals": r"(\w+)\s+(?:is not|does not equal|not equal to)\s*(\w+)",
        "in_values": r"(\w+)\s+(?:is in|are in|within)\s*\((.*?)\)",
        "not_in_values": r"(\w+)\s+(?:is not in|are not in|outside)\s*\((.*?)\)",
        "in_range": r"(\w+)\s+(?:is|are)?\s*(?:between)\s*(\d+)\s+(?:and)\s*(\d+)",
        "starts_with": r"(\w+)\s+(?:starts with|begins with)\s*['\"](.*?)['\"]"
    }

    query = {}
    conditions = []

    # Iterate over patterns to extract multiple matches
    for pattern_name, pattern in patterns.items():
        matches = re.finditer(pattern, user_query, re.IGNORECASE)
        for match in matches:
            if pattern_name in ["greater_than", "less_than"]:
                column = match.group(1).lower()
                value = int(match.group(2))
                if column not in column_names:
                    raise ValueError(f"Column '{column}' not found in metadata.")
                conditions.append({column: {"$gt" if pattern_name == "greater_than" else "$lt": value}})
            elif pattern_name == "equals":
                column = match.group(1).lower()
                value = match.group(2)
                conditions.append({column: value})
            elif pattern_name == "not_equals":
                column = match.group(1).lower()
                value = match.group(2)
                conditions.append({column: {"$ne": value}})
            elif pattern_name in ["in_values", "not_in_values"]:
                column = match.group(1).lower()
                values = [v.strip() for v in match.group(2).split(",")]
                conditions.append({column: {"$in" if pattern_name == "in_values" else "$nin": values}})
            elif pattern_name == "in_range":
                column = match.group(1).lower()
                start, end = int(match.group(2)), int(match.group(3))
                conditions.append({column: {"$gte": start, "$lte": end}})
            elif pattern_name == "starts_with":
                column = match.group(1).lower()
                prefix = match.group(2)
                if column not in column_names:
                    raise ValueError(f"Column '{column}' not found in metadata.")
                conditions.append({column: {"$regex": f"^{prefix}", "$options": "i"}})

    # Combine all conditions into an $and clause if multiple
    if len(conditions) > 1:
        return {"$and": conditions}
    elif conditions:
        return conditions[0]  # Single condition
    else:
        raise ValueError(f"Could not parse the conditions: '{user_query}'.")


def parse_sorting(user_query, collection_name, collection_metadata):

    metadata = collection_metadata[collection_name]
    attributes = metadata.get("categorical_columns", [])
    numeric_columns = metadata.get("numeric_columns", [])
    unique_values = metadata.get("unique_values", {})
    column_names = attributes + numeric_columns
    #print(f"Column Names: {column_names}")

    match = re.search(r"(?:order by|sort by|order)\s+(\w+)\s+(asc|desc)?", user_query, re.IGNORECASE)
    if match:
        column = match.group(1).lower()
        order = 1 if match.group(2) and match.group(2).lower() == "asc" else -1
        #column_names = collection_metadata.get("categorical_columns", []) + collection_metadata.get("numeric_columns", [])
        if column not in column_names:
            raise ValueError(f"Column '{column}' not found in metadata.")
        return {column: order}
    return {}


def parse_display_columns(user_query, collection_name, collection_metadata):

    metadata = collection_metadata[collection_name]
    attributes = metadata.get("categorical_columns", [])
    numeric_columns = metadata.get("numeric_columns", [])
    unique_values = metadata.get("unique_values", {})
    column_names = attributes + numeric_columns

    match = re.search(r"find\s+(.*?)\s+(?:where|order by|sort by|order|$)", user_query, re.IGNORECASE)
    if match:
        # Extract potential columns from the user query
        columns = [col.strip().lower() for col in match.group(1).split(",")]
        selected_columns = [col for col in columns if col in column_names]
        
        if selected_columns:
            # Return projection for explicitly mentioned columns
            projection = {col: 1 for col in selected_columns}
            projection["_id"] = 0  # Exclude MongoDB's default _id field
            return projection
    
    # Default behavior: Show all columns
    all_columns = collection_metadata.get("categorical_columns", []) + collection_metadata.get("numeric_columns", [])
    projection = {col: 1 for col in all_columns}
    projection["_id"] = 0  # Exclude MongoDB's default _id field
    return projection


def parse_aggregation(user_query, collection_name, collection_metadata):
    metadata = collection_metadata[collection_name]
    attributes = metadata.get("categorical_columns", [])
    numeric_columns = metadata.get("numeric_columns", [])
    column_names = attributes + numeric_columns

    # Map user-friendly aggregation functions to MongoDB operators
    agg_function_map = {
        "average": "$avg",
        "sum": "$sum",
        "count": "$count",
        "min": "$min",
        "max": "$max"
    }

    # Pattern to detect aggregation commands
    match = re.search(r"(find\s+(average|sum|count|min|max)\s+(\w+))", user_query, re.IGNORECASE)
    if match:
        agg_func = match.group(2).lower()
        agg_column = match.group(3).lower()

        if agg_func not in agg_function_map:
            raise ValueError(f"Aggregation function '{agg_func}' is not supported.")

        if agg_column not in column_names:
            raise ValueError(f"Column '{agg_column}' not found in metadata.")

        aggregation_pipeline = {
            "$group": {
                "_id": None,
                "result": {agg_function_map[agg_func]: f"${agg_column}"}
            }
        }
        return aggregation_pipeline

    return None

def input_to_mongodb(user_query, collection_name, collection_metadata):
    # Parse aggregation
    aggregation = parse_aggregation(user_query, collection_name, collection_metadata)
    # Parse display columns
    display_columns = parse_display_columns(user_query, collection_name, collection_metadata)
    # Parse conditions
    conditions = parse_conditions(user_query, collection_name, collection_metadata)
    # Parse sorting
    sorting = parse_sorting(user_query, collection_name, collection_metadata)

    if aggregation:
        # Aggregation query
        pipeline = []
        if conditions:
            pipeline.append({"$match": conditions})
        pipeline.append(aggregation)
        if sorting:
            pipeline.append({"$sort": sorting})
        query = f"db.{collection_name}.aggregate({pipeline})"
    else:
        # Standard find query
        query = f"db.{collection_name}.find("
        query += f"{conditions}"  # Add filter conditions
        query += f", {display_columns}" if display_columns else ", {}"  # Add projection (columns to display)
        query += f").sort({sorting})" if sorting else ")"

    return query