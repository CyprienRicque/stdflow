template = """<html>
<body>
<h1 id="name"></h1>
<p id="type"></p>
<h2>Step</h2>
<p id="path"></p>
<p id="version"></p>
<p id="step"></p>
<h2>Columns</h2>
<ul id="columns"></ul>
<h2>Input Files</h2>
<ul id="input_files"></ul>
<h2>Output Files</h2>
<ul id="output_files"></ul>
<h2>Pipeline</h2>
<svg id="pipeline" width="1500" height="500"></svg>

<svg width="5000" height="5000" id="svg">
    <defs>
        <marker id="arrow" markerWidth="4" markerHeight="4" refX="0" refY="2" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,4 L4,2 z" fill="rgba(0, 0, 0, 0.3)"/>
        </marker>
    </defs>
</svg>


<script src="data.js"></script>
<script>
    var currentFile = data.files[data.files.length - 1];

    function addOutputFiles(data) {
        // For each file in the data
        data.files.forEach(function (file) {
            // Add a new output_files property
            file.output_files = [];
            // For each file in the data again
            data.files.forEach(function (possibleOutputFile) {
                // For each input_file of the possible output file
                possibleOutputFile.input_files.forEach(function (inputFile) {
                    // If the inputFile's uuid matches the file's uuid
                    if (inputFile.uuid === file.uuid) {
                        // Then this possibleOutputFile is an output of file
                        file.output_files.push(possibleOutputFile);
                    }
                });
            });
        });
    }

    addOutputFiles(data);


    let maxDepth = 0;
    const colWidth = 400;


    updatePage();

    function setup_file_text(a, fileInData) {
        a.textContent = fileInData.file_name + ' (' + fileInData.step.path + '/' + fileInData.step.step_name + ')' + ' (' + fileInData.uuid + ')';
        a.href = '#';
        a.onclick = function () {
            currentFile = fileInData;
            updatePage();
            return false;
        };
    }

    function updatePage() {
        function calculateDepths(file, depth = 0) {
            if (file.depth === undefined || file.depth < depth) {
                console.log(file.uuid.substring(0, 5), depth)
                file.depth = depth;
            }
            maxDepth = Math.max(maxDepth, depth);

            // Use output_files instead of input_files
            file.output_files.forEach(function (outputFile) {
                calculateDepths(outputFile, depth + 1);
            });
        }

        data.files.forEach(function (file) {
            if (!file.input_files || file.input_files.length === 0) {
                calculateDepths(file);
            }
        });    // Multiple all depth per 2
        data.files.forEach(function (file) {
            file.depth = maxDepth - file.depth;
        });

        document.getElementById('name').textContent = currentFile.file_name;
        document.getElementById('type').textContent = 'Type: ' + currentFile.file_type;
        document.getElementById('path').textContent = 'Attributes: ' + currentFile.step.path;
        document.getElementById('version').textContent = 'Version: ' + currentFile.step.version;
        document.getElementById('step').textContent = 'Step: ' + currentFile.step.step_name;

        var columnsList = document.getElementById('columns');
        columnsList.innerHTML = '';
        currentFile.columns.forEach(function (column) {
            var li = document.createElement('li');
            li.textContent = column.name + ' (' + column.type + '): ' + column.description;
            columnsList.appendChild(li);
        });

        var inputFilesList = document.getElementById('input_files');
        inputFilesList.innerHTML = '';
        currentFile.input_files.forEach(function (inputFile) {
            var li = document.createElement('li');
            var a = document.createElement('a');
            var fileInData = data.files.find(function (file) {
                return file.uuid === inputFile.uuid;
            });
            setup_file_text(a, fileInData);
            li.appendChild(a);
            inputFilesList.appendChild(li);
        });

        var outputFilesList = document.getElementById('output_files');
        outputFilesList.innerHTML = '';
        currentFile.output_files.forEach(function (outputFile) {
            var li = document.createElement('li');
            var a = document.createElement('a');
            var fileInData = data.files.find(function (file) {
                return file.uuid === outputFile.uuid;
            });
            setup_file_text(a, fileInData);
            li.appendChild(a);
            outputFilesList.appendChild(li);
        });
    }

    var svg = document.getElementById('pipeline');
    svg.style.width = (maxDepth + 1) * colWidth + 'px';  // Adjust svg width based on maxDepth
    data.files.forEach(function (file) {
        var a = document.createElementNS('http://www.w3.org/2000/svg', 'a');
        a.href = '#';
        a.onclick = function () {
            currentFile = file;
            updatePage();
            return false;
        };

        // take absolute value
        // var depth = Math.abs(file.depth);

        // first 3 chars of str
        var text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.textContent = file.file_name + ' (' + file.step.path + '/' + file.step.step_name + ')' + ' (' + file.uuid.substring(0, 4) + ')';
        var textX = (maxDepth - file.depth) * colWidth + colWidth / 2;


        text.setAttribute('x', textX);  // Adjust 'x' attribute based on maxDepth - file.depth
        text.setAttribute('text-anchor', 'middle');

        text.setAttribute('y', data.files.indexOf(file) * 20 + 20);  // Position nodes vertically based on order of appearance
        a.appendChild(text);

        svg.appendChild(a);

        file.input_files.forEach(function (inputFile) {
            var inputFileInData = data.files.find(function (f) {
                return f.uuid === inputFile.uuid;
            });
            var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x2', (maxDepth - file.depth) * colWidth + (colWidth / 15));
            line.setAttribute('y2', data.files.indexOf(file) * 20 + 15);
            console.log("set", inputFileInData, "for ", inputFile.uuid.substring(0, 5))
            console.log("set", inputFileInData.uuid.substring(0, 5), inputFileInData.depth)
            line.setAttribute('x1', (maxDepth - (inputFileInData.depth - 1)) * colWidth - (colWidth / 15));
            line.setAttribute('y1', data.files.indexOf(inputFileInData) * 20 + 15);
            line.style.stroke = 'rgba(0,0,0,0.3)';
            line.style.strokeWidth = '2px';
            // make it look like an arrow
            line.setAttribute('marker-end', 'url(#arrow)');


            svg.appendChild(line);
        });
    });
</script>


</body>
</html>
"""
