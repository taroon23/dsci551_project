import pandas as pd
import random
import re

def create_sample_query(query_type, table_name, metadata_store):
    
    #column_names, attributes, measures, unique_elements

    if table_name not in metadata_store:
        return None, f"Table '{table_name}' not found in metadata store."
    
    metadata = metadata_store[table_name]
    
    attributes = metadata["attributes"]
    measures = metadata["measures"]
    column_names = metadata["column_names"]
    unique_elements = metadata["unique_elements"]

    agg_functions = ['Sum', 'Avg', 'Min', 'Max']
    comparison_operators = ['>', '<', '>=', '<=', '=', '!=']
    sort_directions = ['asc', 'desc']

    def generate_aggregate_queries(aggregation_function=None, include_group_by=False):
        queries = []
        for _ in range(5):
            group_columns = random.sample(attributes, random.randint(1, len(attributes) // 2 + 1))
            group_columns_text = ', '.join(group_columns)
            chosen_measure = random.choice(measures)
            aggregation_text = f"{aggregation_function}({chosen_measure}) as {aggregation_function}_{chosen_measure}" if aggregation_function else '*'
            group_by_clause = f" group by {group_columns_text}" if include_group_by else ''
            queries.append(f"Select {group_columns_text}, {aggregation_text} from {table_name}{group_by_clause}")
        return queries

    def generate_condition_queries():
        queries = []
        for _ in range(5):
            conditions = [
                f"{column}='{random.choice(unique_elements[column])}'"
                for column in random.sample(attributes, 2)
            ]
            selected_columns = random.sample(column_names, random.randint(1, len(column_names) // 2 + 1))
            queries.append(
                f"Select {', '.join(selected_columns)} from {table_name} where {' and '.join(conditions)}"
            )
        return queries

    def generate_sorting_queries():
        queries = []
        for _ in range(5):
            selected_columns = random.sample(column_names, random.randint(1, len(column_names) // 2 + 1))
            sort_column = random.choice(selected_columns)
            sort_order = random.choice(sort_directions)
            queries.append(f"Select {', '.join(selected_columns)} from {table_name} order by {sort_column} {sort_order}")
        return queries

    def generate_having_queries():
        queries = []
        for _ in range(5):
            grouped_columns = random.sample(attributes, random.randint(1, len(attributes) // 2 + 1))
            grouped_columns_text = ', '.join(grouped_columns)
            chosen_agg_function = random.choice(agg_functions)
            chosen_agg_measure = random.choice(measures)
            operator = random.choice(comparison_operators)
            threshold = random.randint(1, 1000)
            queries.append(
                f"Select {grouped_columns_text}, {chosen_agg_function}({chosen_agg_measure}) as {chosen_agg_measure}_{chosen_agg_function} "
                f"from {table_name} group by {grouped_columns_text} having {chosen_agg_measure}_{chosen_agg_function} {operator} {threshold}"
            )
        return queries

    # Map query types to their respective generator functions
    query_handlers = {
        'group by': lambda: generate_aggregate_queries(include_group_by=True),
        'sum': lambda: generate_aggregate_queries('Sum', include_group_by=True),
        'avg': lambda: generate_aggregate_queries('Avg', include_group_by=True),
        'min': lambda: generate_aggregate_queries('Min', include_group_by=True),
        'max': lambda: generate_aggregate_queries('Max', include_group_by=True),
        'where': generate_condition_queries,
        'order by': generate_sorting_queries,
        'having': generate_having_queries,
    }

    # Return generated queries based on the query type
    return query_handlers.get(query_type.lower(), lambda: [])()

def selecting(query, table_name, metadata_store):

    # Fetch metadata for the given table
    if table_name not in metadata_store:
        return None, f"Table '{table_name}' not found in metadata store."
    
    metadata = metadata_store[table_name]
    attributes = metadata["attributes"]
    measures = metadata["measures"]
    column_names = metadata["column_names"]

    patterns = {
        "total_group_by": r"(?:sum|total|sum of|total of all|sum of all) (.+?)(?:by|grouped by|group by|for each|per|for every|of each) (.+?)(?:$|where|limit|ordered|sort)",
        "average_group_by": r"(?:mean|avg|average|average of|mean of|avg of) (.+?)(?:by|group by|grouped by|for every|of each|for each|per) (.+?)(?:$|ordered|where|sort|limit)",
        "min_group_by": r"(?:smallest|min|lowest|min of|minimum|minimum of) (.+?)(?:by|for each|group by|of each|for every|grouped by|per) (.+?)(?:$|ordered|where|limit|sort)",
        "max_group_by": r"(?:maximum|max|largest|max of|maximum of|biggest) (.+?)(?:grouped by|by|for every|group by|of each|for each|per) (.+?)(?:$|sort|limit|ordered|where)",
        "count_group_by": r"(?:number|count) (.+?)(?:group by|grouped by|by|for each|for every|of each|per) (.+?)(?:$|ordered|sort|where|limit)",
        
        "total": r"(?:sum|total|total of all|sum of|sum of all) (.+?)(?:$|ordered|sort|limit|where)",
        "average": r"(?:mean|average|avg|mean of|avg of|average of) (.+?)(?:$|limit|where|ordered|sort)",
        "min": r"(?:minimum|min|min of|smallest|minimum of|lowest|lowest of) (.+?)(?:$|limit|sort|ordered|where)",
        "max": r"(?:maximum|max|max of|largest|maximum of|biggest|highest) (.+?)(?:$|where|limit|ordered|sort)",
        
        "count": r"(?:number|count) (.+?)(?:$|limit|sort|where|ordered)",
        "Select": r"(?:Find|Select|List|Give|Show|Provide) (.+?)(?:$|limit|ordered|sort|where)",
        "order_asc": r"(?:first|top) (.+?)(?:by|ordered by|based on) (.+?)(?:$|where|sort|ordered|limit)",
        "order_desc": r"(?:last|bottom) (.+?)(?:by|ordered by|based on) (.+?)(?:$|limit|sort|ordered|where)"
    }


    # Detect patterns
    detected_pattern = None
    detected_groups = None
    for pattern_name, pattern_regex in patterns.items():
        match = re.search(pattern_regex, query, re.IGNORECASE)
        if match:
            detected_pattern = pattern_name
            detected_groups = match.groups()
            break

    if not detected_pattern:
        return None, "No valid pattern detected."

    # Helper function for matching columns and measures
    def match_columns_or_measures(target_list, text):
        matched = []
        for item in target_list:
            item_lower = item.lower()
            text_lower = text.lower()
            if item_lower in text_lower or item_lower.replace('_', ' ') in text_lower:
                matched.append(item)
        return ', '.join(matched)

    # SQL generation logic
    if detected_pattern.endswith("_group_by"):
        metric = detected_groups[0]
        group_by = detected_groups[1]

        measure_function = detected_pattern.split("_")[0]
        function_map = {"total": "SUM", "average": "AVG", "min": "MIN", "max": "MAX", "count": "COUNT"}

        selected_measures = match_columns_or_measures(measures, metric)
        group_by_columns = match_columns_or_measures(attributes, group_by)

        if selected_measures and group_by_columns:
            sql_query = f"SELECT {group_by_columns}, {function_map[measure_function]}({selected_measures}) FROM {table_name} GROUP BY {group_by_columns}"
        else:
            # Handle cases where group_by or metric columns are not found
            if not group_by_columns:
                group_by_columns = "*"
            if not selected_measures:
                selected_measures = "*"
            sql_query = f"SELECT {group_by_columns}, {function_map[measure_function]}({selected_measures}) FROM {table_name} GROUP BY {group_by_columns}"

        return detected_pattern, sql_query

    if detected_pattern in ["total", "average", "min", "max", "count"]:
        metric = detected_groups[0]
        measure_function = detected_pattern
        function_map = {"total": "SUM", "average": "AVG", "min": "MIN", "max": "MAX", "count": "COUNT"}

        selected_measures = match_columns_or_measures(measures if measure_function != "count" else column_names, metric)
        if selected_measures:
            sql_query = f"SELECT {function_map[measure_function]}({selected_measures}) FROM {table_name}"
        else:
            # Default case when column isn't found for count
            if measure_function == "count":
                sql_query = f"SELECT COUNT(*) FROM {table_name}"
            else:
                return None, "No valid metric detected for aggregation."
        return detected_pattern, sql_query

    if detected_pattern == "Select":
        columns = detected_groups[0]
        selected_columns = match_columns_or_measures(column_names, columns)
        if selected_columns:
            sql_query = f"SELECT {selected_columns} FROM {table_name}"
        else:
            # Default case when column isn't found for Select
            sql_query = f"SELECT * FROM {table_name}"
        return detected_pattern, sql_query

    if detected_pattern in ["order_desc", "order_asc"]:
        columns, order_by = detected_groups
        limit = re.findall(r'\d+', columns)[0] if re.search(r'\d+', columns) else "1"
        order_direction = "DESC" if detected_pattern == "order_desc" else "ASC"

        selected_columns = match_columns_or_measures(column_names, columns)
        order_by_column = match_columns_or_measures(column_names, order_by)

        if selected_columns and order_by_column:
            sql_query = f"SELECT {selected_columns} FROM {table_name} ORDER BY {order_by_column} {order_direction} LIMIT {limit}"
        return detected_pattern, sql_query

    return None, "Pattern detected but unable to generate SQL. Check query or column names."


def filtering(query, table_columns):

    # Extract filtering conditions from the query
    filter_pattern = r"(?:where|when|whose|with|having) (.+?)(?:$|limit|limited|limit to|sort|sorted|arranged|ordered|order)"
    extracted_filter_text = re.search(filter_pattern, query, re.IGNORECASE)
    extracted_filter_text = extracted_filter_text.groups()[0].lower() if extracted_filter_text else ""

    # Add leading space for easier parsing and prepare the regex pattern for column matching
    formatted_text = f' {extracted_filter_text}'
    column_split_pattern = '|'.join(
        f'({re.escape(" " + col)})' for col in table_columns + ["and", "but"]
    )
    parsed_parts = [segment.strip() for segment in re.split(column_split_pattern, formatted_text) if segment]

    # Merge segments for 'between' conditions
    idx = 0
    while idx < len(parsed_parts):
        if "between" in parsed_parts[idx].lower() and idx + 2 < len(parsed_parts):
            parsed_parts[idx] += f" {parsed_parts.pop(idx + 1)} {parsed_parts.pop(idx + 1)}"
            parsed_parts[idx] = parsed_parts[idx][parsed_parts[idx].lower().index("between"):]
        else:
            idx += 1

    # Define comparison patterns and aggregate function mappings
    comparison_patterns = {
        r"(?:is greater than or equal to|greater than or equal to)": ">=",
        r"(?:is lesser than or equal to|is less than or equal to|lesser than or equal to|less than or equal to)": "<=",
        r"(?:is greater than|is great than|greater than|great than|more than|is above|above)": ">",
        r"(?:is lesser than|is less than|lesser than|less than|is below|below)": "<",
        r"(?:is equal to|equal to)": "=",
        r"(?:is not equal to|not equal to)": "!=",
        r"\bis\b": "="
    }

    aggregate_functions_map = {
        "Avg": ["average", "avg", "mean"],
        "Sum": ["sum", "total"],
        "Min": ["min", "minimum", "lowest", "smallest"],
        "Max": ["max", "maximum", "biggest", "largest"],
    }
    aggregate_keywords = [word for func_words in aggregate_functions_map.values() for word in func_words]

    # Initialize lists for filtering conditions
    condition_where = []
    condition_having = []
    active_column = None
    is_aggregate = False
    aggregate_function = None

    for part in parsed_parts:
        if part.lower() in ["and", "but", ""]:
            continue

        # Identify aggregate conditions
        if not is_aggregate:
            matched_aggregate = next(
                (word for word in aggregate_keywords if re.search(rf"\b{word}\b", part, re.IGNORECASE)), 
                None
            )
            if matched_aggregate:
                is_aggregate = True
                aggregate_function = next(func for func, words in aggregate_functions_map.items() if matched_aggregate in words)
                continue

        # Match column names
        if part.lower() in table_columns:
            active_column = part.lower()
            if is_aggregate:
                active_column = f"{aggregate_function}({active_column})"
                is_aggregate = False
            continue

        # Process filtering conditions
        if active_column and active_column != part.lower():
            condition_expr = part

            # Replace natural language expressions with SQL-compatible operators
            for regex, operator in comparison_patterns.items():
                condition_expr = re.sub(regex, operator, condition_expr, flags=re.IGNORECASE)

            # Add quotes to non-numeric values and date formats
            condition_expr = re.sub(r"([=<>!]=?)\s*([^\d\s]+)", r"\1 '\2'", condition_expr)
            condition_expr = re.sub(r"\b(\d{4})-(\d{2})-(\d{2})\b", r"'\g<0>'", condition_expr)

            # Combine column and condition expression
            full_condition = f"{active_column} {condition_expr}"

            # Determine whether the condition goes to WHERE or HAVING
            if active_column in table_columns:
                condition_where.append(full_condition)
            else:
                condition_having.append(full_condition)

    return condition_where, condition_having


def sortlimit(query, column_names):
    # Define patterns
    pattern = {
        'sort': r"(?:arranged|sort|sorted|ordered by|order by|arrange) (.+?)(?:$|limit|limited|limit to|skip|offset)",
        'limit': r"(?:limit|limited to|limit to) (.+?)(?:$|arranged|sort|sorted|ordered by|order by|arrange|skip|offset)",
        'offset': r"(?:skip|offset) (.+?)(?:$|arranged|sort|sorted|ordered by|order by|arrange|limit|limited to|limit)",
    }
    
    # Detect parts from the query
    pattern_found = {}
    for pattern_name, pattern in pattern.items():
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            pattern_found[pattern_name] = match.groups()

    # Initialize output parts
    limit_part = ''
    order_part = ''
    offset_part = ''

    # Process detected parts
    for query_part, text in pattern_found.items():
        if query_part == 'sort':
            order_by_columns = []
            for col in column_names:
                if col in text[0].lower() or col.replace('_', ' ') in text[0].lower():
                    order_by_columns.append(col)
            if 'descending' in text[0].lower():
                order_by_type = 'DESC'
            else:
                order_by_type = 'ASC'

            order_part = ' order by ' + ','.join(order_by_columns) + ' ' + order_by_type

        if query_part == 'limit':
            numbers = re.findall(r'\d+', text[0])
            if numbers:
                limit_part = ' limit ' + numbers[0]

        if query_part == 'offset':
            numbers = re.findall(r'\d+', text[0])
            if numbers:
                offset_part = ' offset ' + numbers[0]

    return order_part + limit_part + offset_part

def input_to_sql(user_query, table_name, metadata_store):
    """
    Translates a natural language query into an SQL query using the table's metadata.
    """
    # Retrieve metadata for the given table
    if table_name not in metadata_store:
        raise ValueError(f"Table '{table_name}' not found in metadata store.")
    
    metadata = metadata_store[table_name]
    column_names = metadata.get("column_names", [])


    pattern_select, select = selecting(user_query, table_name, metadata_store)
    filter_output = filtering(user_query, column_names)
    
    # # Build WHERE and HAVING clauses
    where_filter = f" WHERE {' AND '.join(filter_output[0])}" if filter_output[0] else ''
    having_filter = f" HAVING {' AND '.join(filter_output[1])}" if filter_output[1] else ''

    sort_limit = sortlimit(user_query, column_names)

    # Construct final SQL query
    final_query = select
    if pattern_select not in ['order_desc', 'order_asc']:
        if 'GROUP BY' in final_query:
            final_query = final_query.replace('GROUP BY', where_filter + ' GROUP BY')
        else:
            final_query += ' ' + where_filter
        final_query += ' ' + having_filter + ' ' + sort_limit
    else:
        if 'ORDER BY' in pattern_select:
            final_query = final_query.replace('ORDER BY', where_filter + ' ORDER BY')

    return final_query
