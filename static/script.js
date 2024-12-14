let selectedTable = null;

// Show Dataset Section After Selecting SQL or NoSQL

function showDatasetSection() {
    const dbType = document.querySelector('input[name="dbType"]:checked');
    if (!dbType) {
        alert("Please select a database type.");
        return;
    }

    if (dbType.value === "SQL") {
        document.getElementById('db-selection').classList.add('hidden');
        document.getElementById('dataset-selection').classList.remove('hidden');
        document.getElementById('dataset-selection').querySelector('h3').innerText = "SQL Database";
    } else if (dbType.value === "NoSQL") {
        document.getElementById('db-selection').classList.add('hidden');
        document.getElementById('nosql-section').classList.remove('hidden');
        document.getElementById('nosql-section').querySelector('h3').innerText = "NoSQL Database";
    } else {
        alert("Unsupported database type.");
    }
}

// Handle Table Selection
function handleTableSelection() {
    const tableSelect = document.getElementById('table-select').value;
    const uploadSection = document.getElementById('upload-section');

    if (tableSelect === 'upload') {
        uploadSection.classList.remove('hidden');
        selectedTable = null; // Reset selected table for uploads
    } else {
        uploadSection.classList.add('hidden');
        selectedTable = tableSelect; // Set the selected table
    }
}

// Upload CSV Function
async function uploadCsv() {
    const fileInput = document.getElementById('file-input');
    if (!fileInput.files.length) {
        alert("Please select a file to upload.");
        return;
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    const response = await fetch('/upload-csv', {
        method: 'POST',
        body: formData
    });

    const result = await response.json();
    if (response.ok) {
        alert(result.message);
        selectedTable = result.metadata.table_name; // Set the new table name for preview
        //return jsonify({"message": f"Table '{metadata['table_name']}' uploaded successfully.", "metadata": metadata})
    } else {
        alert(`Error: ${result.error}`);
    }
}

// Continue Button Action (Preview Data)
async function continueTableSelection() {
    if (!selectedTable) {
        alert("Please select a table or upload your own table.");
        return;
    }

    const response = await fetch(`/preview-table?table=${selectedTable}`);
    const data = await response.json();

    const tablePreview = document.getElementById('data-preview');
    if (response.ok) {
        if (data.length > 0) {
            // Generate HTML table dynamically
            let tableHtml = '<table border="1"><thead><tr>';
            const columns = Object.keys(data[0]);

            // Add column headers
            columns.forEach((col) => {
                tableHtml += `<th>${col}</th>`;
            });
            tableHtml += '</tr></thead><tbody>';

            // Add rows
            data.forEach((row) => {
                tableHtml += '<tr>';
                columns.forEach((col) => {
                    tableHtml += `<td>${row[col]}</td>`;
                });
                tableHtml += '</tr>';
            });
            tableHtml += '</tbody></table>';

            tablePreview.innerHTML = tableHtml;

            // Show Query Section
            document.getElementById('query-section').classList.remove('hidden');
        } else {
            tablePreview.innerHTML = '<p>No data available in the table.</p>';
        }
    } else {
        tablePreview.innerHTML = `<p>Error: ${data.error || "Unknown error occurred."}</p>`;
    }
}

// Show Query Options
function showQueryOptions(type) {
    const queryOptions = document.getElementById('query-options');
    queryOptions.classList.remove('hidden');

    if (type === 'random') {
        queryOptions.innerHTML = `
            <label>Select a query type:</label>
            <select id="random-query">
                <option value="group by">Group By</option>
                <option value="sum">Sum</option>
                <option value="avg">Average</option>
                <option value="min">Min</option>
                <option value="max">Max</option>
                <option value="where">Where</option>
                <option value="order by">Order By</option>
                <option value="having">Having</option>
            </select>
            <button class="button" onclick="executeRandomQuery()">Generate Query</button>
        `;
    } else if (type === 'convert') {
        queryOptions.innerHTML = `
            <label>Enter your query:</label>
            <input type="text" id="convert-query" placeholder="Enter natural language query">
            <button class="button" onclick="executeConvertQuery()">Submit</button>
        `;
    }
}


// Execute Random Query
function executeRandomQuery() {
    const queryType = document.getElementById('random-query').value;
    alert(`Random Query Selected: ${queryType}`);
    // Replace with backend function
}

// Execute Convert Query
function executeConvertQuery() {
    const userInput = document.getElementById('convert-query').value;
    if (!userInput) {
        alert("Please enter a query.");
        return;
    }
    alert(`Convert Query Input: ${userInput}`);
    // Replace with backend function
}

// Execute random query generation
async function executeRandomQuery() {
    const queryType = document.getElementById("random-query").value;

    const response = await fetch("/generate-random-query", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            table_name: selectedTable,
            query_type: queryType, // Pass the query type selected by the user
        }),
    });

    const result = await response.json();
    if (response.ok) {
        const queryOptions = document.getElementById("query-options");
        queryOptions.innerHTML = `<h4>Generated Queries:</h4><pre>${result.queries.join("\n")}</pre>`;
    } else {
        alert(`Error: ${result.error}`);
    }
}

// Execute Convert to Query
async function executeConvertQuery() {
    const userInput = document.getElementById('convert-query').value;

    if (!userInput || !selectedTable) {
        alert("Please select a table and enter a query.");
        return;
    }

    const response = await fetch('/convert-to-query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            table_name: selectedTable,
            user_input: userInput
        })
    });

    const result = await response.json();
    if (response.ok) {
        const queryOptions = document.getElementById('query-options');
        queryOptions.innerHTML = `<h4>Generated Query:</h4><pre>${result.query}</pre>`;
    } else {
        alert(`Error: ${result.error}`);
    }
}


async function processUserQuery() {
    const userInput = document.getElementById('query-input').value;

    if (!userInput || !selectedTable) {
        alert("Please select a table and enter a query.");
        return;
    }

    const response = await fetch('/process_query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            query: userInput,
            table_name: selectedTable
        })
    });

    const result = await response.json();
    const queryResultDiv = document.getElementById('query-result');
    const tablePreview = document.getElementById('data-preview');

    if (response.ok) {
        if (result.samples) {
            // Case 1: Random query generation
            queryResultDiv.innerHTML = `<h4>Generated Random Queries:</h4><pre>${result.samples.join("\n")}</pre>`;
            tablePreview.innerHTML = '<p>No data preview available for random query generation.</p>';
        } else if (result.translated_query) {
            // Case 2: SQL query translation and execution
            queryResultDiv.innerHTML = `<h4>Generated Query:</h4><pre>${result.translated_query}</pre>`;

            if (result.data && result.data.length > 0) {
                // Generate HTML table dynamically for query results
                let tableHtml = '<table border="1"><thead><tr>';
                const columns = Object.keys(result.data[0]);

                // Add column headers
                columns.forEach((col) => {
                    tableHtml += `<th>${col}</th>`;
                });
                tableHtml += '</tr></thead><tbody>';

                // Add rows
                result.data.forEach((row) => {
                    tableHtml += '<tr>';
                    columns.forEach((col) => {
                        tableHtml += `<td>${row[col]}</td>`;
                    });
                    tableHtml += '</tr>';
                });
                tableHtml += '</tbody></table>';

                tablePreview.innerHTML = tableHtml;
            } else {
                tablePreview.innerHTML = '<p>No data returned from the query.</p>';
            }
        }
    } else {
        queryResultDiv.innerHTML = `<p>Error: ${result.error}</p>`;
        tablePreview.innerHTML = '';
    }
}


let selectedCollection = null;

function handleNoSQLCollectionSelection() {
    const collectionSelect = document.getElementById('nosql-collection-select').value;
    const uploadSection = document.getElementById('nosql-upload-section');

    if (collectionSelect === 'upload') {
        uploadSection.classList.remove('hidden');
        selectedCollection = null; // Reset selected collection for uploads
    } else {
        uploadSection.classList.add('hidden');
        selectedCollection = collectionSelect; // Set selected collection
        console.log(`Selected Collection: ${selectedCollection}`);
    }
}


async function uploadNoSQL() {
    const fileInput = document.getElementById('nosql-file-input');
    if (!fileInput.files.length) {
        alert("Please select a file to upload.");
        return;
    }

    const formData = new FormData();
    formData.append("file", fileInput.files[0]);

    const response = await fetch("/upload-nosql", {
        method: "POST",
        body: formData,
    });

    const result = await response.json();
    if (response.ok) {
        alert(result.message);

        // Update selectedCollection using the correct key
        selectedCollection = Object.keys(result.metadata)[0]; // Gets the uploaded collection name
        console.log(`Selected Collection: ${selectedCollection}`);

        //selectedCollection = result.collection_name; // Set new collection name

        //return jsonify({"message": f"Collection '{collection_name}' uploaded successfully.", "metadata": metadata})
        //selectedTable = result.metadata.table_name; // Set the new table name for preview
        //return jsonify({"message": f"Table '{metadata['table_name']}' uploaded successfully.", "metadata": metadata})

    } else {
        alert(`Error: ${result.error}`);
    }
}
async function continueNoSQLSelection() {
    if (!selectedCollection) {
        alert("Please select a collection or upload your own dataset.");
        return;
    }

    const response = await fetch(`/preview-nosql?collection=${selectedCollection}`);
    const data = await response.json();

    const tablePreview = document.getElementById("data-preview");
    if (response.ok) {
        if (data.length > 0) {
            let tableHtml = '<table border="1"><thead><tr>';
            const columns = Object.keys(data[0]);

            columns.forEach((col) => {
                tableHtml += `<th>${col}</th>`;
            });
            tableHtml += '</tr></thead><tbody>';

            data.forEach((row) => {
                tableHtml += '<tr>';
                columns.forEach((col) => {
                    tableHtml += `<td>${row[col]}</td>`;
                });
                tableHtml += '</tr>';
            });
            tableHtml += '</tbody></table>';
            tablePreview.innerHTML = tableHtml;

            // Show Query Section
            document.getElementById('nosql-query-section').classList.remove('hidden');

        } else {
            tablePreview.innerHTML = '<p>No data available in the collection.</p>';
        }
    } else {
        tablePreview.innerHTML = `<p>Error: ${data.error}</p>`;
    }
}

async function processNoSQLQuery() {
    const userInput = document.getElementById('nosql-query-input').value;

    console.log("Selected Collection in processNoSQLQuery:", selectedCollection);
    console.log("User Input in processNoSQLQuery:", userInput);

    if (!userInput || !selectedCollection) {
        alert("Please select a collection and enter a query.");
        return;
    }

    const response = await fetch('/process-nosql-query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            query: userInput,
            collection: selectedCollection
        })
    });

    const queryResultDiv = document.getElementById('nosql-query-result');
    const tablePreview = document.getElementById('data-preview');
    const result = await response.json();

    console.log("Response from /process-nosql-query:", result);

    if (response.ok) {
        if (result.sample_query) {
            // Case 1: Random query generation
            queryResultDiv.innerHTML = `<h4>Generated Sample Query:</h4><pre>${JSON.stringify(result.sample_query, null, 2)}</pre>`;
            tablePreview.innerHTML = '<p>No data preview available for sample query generation.</p>';
        } else if (result.query) {
            // Case 2: MongoDB query translation and execution
            queryResultDiv.innerHTML = `<h4>Generated Query:</h4><pre>${JSON.stringify(result.query, null, 2)}</pre>`;

            if (result.data && result.data.length > 0) {
                // Generate HTML table dynamically for query results
                let tableHtml = '<table border="1"><thead><tr>';
                const columns = Object.keys(result.data[0]);

                // Add column headers
                columns.forEach((col) => {
                    tableHtml += `<th>${col}</th>`;
                });
                tableHtml += '</tr></thead><tbody>';

                // Add rows
                result.data.forEach((row) => {
                    tableHtml += '<tr>';
                    columns.forEach((col) => {
                        tableHtml += `<td>${row[col]}</td>`;
                    });
                    tableHtml += '</tr>';
                });
                tableHtml += '</tbody></table>';

                tablePreview.innerHTML = tableHtml;
            } else {
                tablePreview.innerHTML = '<p>No data returned from the query.</p>';
            }
        }
    } else {
        queryResultDiv.innerHTML = `<p>Error: ${result.error}</p>`;
        tablePreview.innerHTML = '';
    }
}

