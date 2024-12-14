// Show Dataset Section
function showDatasetSection() {
    const dbType = document.querySelector('input[name="dbType"]:checked');

    if (!dbType) {
        alert("Please select a database type.");
        return;
    }

    document.getElementById('db-title').textContent = `${dbType.value} Database`;
    document.getElementById('db-selection').classList.add('hidden');
    document.getElementById('dataset-selection').classList.remove('hidden');
}

// Handle Table Selection
function handleTableSelection() {
    const tableSelect = document.getElementById('table-select').value;
    const uploadSection = document.getElementById('upload-section');

    if (tableSelect === 'upload') {
        uploadSection.classList.remove('hidden');
        document.getElementById('upload-form').onsubmit = async function (event) {
            event.preventDefault();
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
            } else {
                alert(`Error: ${result.error}`);
            }
        };
    } else {
        uploadSection.classList.add('hidden');
        previewTable(tableSelect);
    }
}

// Preview Table Data
async function previewTable(tableName) {
    const response = await fetch(`/preview-table?table=${tableName}`);
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
                <option value="random">Random</option>
                <option value="group by">Group By</option>
                <option value="having">Having</option>
                <option value="order by">Order By</option>
                <option value="where">Where</option>
                <option value="join">Join</option>
            </select>
            <button class="button" onclick="executeRandomQuery()">Execute</button>
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
