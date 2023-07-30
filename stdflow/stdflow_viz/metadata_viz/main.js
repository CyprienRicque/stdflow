let currentSelectedNode = null;  // Add this line to keep track of selected node
let filePath = "../metadata.json"

function updatePage(jsonData, clickedNodeId) {
    drawGraph(jsonData, clickedNodeId);
    show_details(jsonData, clickedNodeId);
}

function drawGraph(jsonData, selectedNodeId) {
    let nodesInView = [];
    let edgesInView = [];
    let depth = parseInt(document.getElementById('depth').value);
    let ignoreStep = document.getElementById('ignoreStep').value;

    let inputNodes = findInputNodes(jsonData.files, selectedNodeId, depth, ignoreStep);
    let outputNodes = findOutputNodes(jsonData.files, selectedNodeId, depth, ignoreStep);


    // reset levels
    levels = {};

    // in your drawGraph function, after calling findInputNodes and findOutputNodes
    inputNodes.forEach(node => {
        levels[node.uuid] = (-node.level); // using negative to place input nodes to the left
    });

    console.log(outputNodes)

    outputNodes.forEach(node => {
        levels[node.uuid] = (node.level);
    });
    console.log(levels)


    jsonData.files.forEach((file) => {
        if (file.uuid === selectedNodeId || inputNodes.some(node => node.uuid === file.uuid) || outputNodes.some(node => node.uuid === file.uuid)) {
            console.log(`checking ${file.step.step_name} against ${ignoreStep}`);
            if (file.step.step_name === ignoreStep) {
                console.log(`Ignoring ${file.uuid} because it is from step ${ignoreStep}`);
            } else {
                nodesInView.push({
                    id: file.uuid,
                    level: levels[file.uuid] || 0,
                    label: `${file.file_name}.${file.file_type}\nPath: ${file.step.path}\nStep: ${file.step.step_name || 'N/A'}`,
                    data: file,
                    color: file.uuid === selectedNodeId ? 'rgb(243,213,163)' : 'rgb(166,200,222)', // Adding alpha channel to colors
                    borderWidth: 2, // Adding border
                    borderColor: '#000000', // Border color
                });
            }
        }
    });

    jsonData.files.forEach((file) => {
        file.input_files.forEach((inputFile) => {
            if ((nodesInView.some(node => node.id === file.uuid) && nodesInView.some(node => node.id === inputFile.uuid))) {
                edgesInView.push({
                    from: inputFile.uuid,
                    to: file.uuid,
                    arrows: 'to'
                });
            }
        });
    });


    let nodes = new vis.DataSet(nodesInView);
    let edges = new vis.DataSet(edgesInView);

    let container = document.getElementById('mynetwork');
    let data = {
        nodes: nodes,
        edges: edges
    };
    let options = {
        physics: {
            stabilization: true,
            barnesHut: {
                gravitationalConstant: -10000,
                springConstant: 0.001,
                springLength: 100
            }
        },
        layout: {
            hierarchical: {
                direction: 'UD', // Left to right
            }
        },
        interaction: {
            zoomSpeed: 0.3 // Lower value for less sensitivity
        },
        nodes: {
            shape: 'box',
            font: {
                size: 10,
                face: 'Arial'
            },
            scaling: {
                label: {
                    min: 8,
                    max: 20
                }
            },
            margin: 10
        }
    };

    let network = new vis.Network(container, data, options);


    // Handling the click event
    // Handling the click event
    network.on("click", function (params) {
        params.event = "[original event]";
        let clickedNodeId = params.nodes[0];
        if (clickedNodeId) {
            currentSelectedNode = clickedNodeId; // Update currentSelectedNode when a node is clicked

            // Redraw the graph with the clicked node as the selected node
            updatePage(jsonData, clickedNodeId, nodes);

        }
    });
}

// Call the drawGraph function initially with no selected node
fetch(filePath)
    .then(response => response.json())
    .then(jsonData => {
        currentSelectedNode = jsonData.files[0].uuid; // Initialize currentSelectedNode
        updatePage(jsonData, currentSelectedNode);
    })
    .catch(error => console.log('Error:', error));

document.getElementById('updateGraph').addEventListener('click', function () {
    fetch(filePath)
        .then(response => response.json())
        .then(jsonData => {
            updatePage(jsonData, currentSelectedNode); // Use currentSelectedNode here
        })
        .catch(error => console.log('Error:', error));
});

let levels = {};

function findInputNodes(allNodes, nodeId, depth, ignoreStep, nodeDepth = 1) {
    let inputNodes = [];
    if (depth > 0) {
        let currentNode = allNodes.find(node => node.uuid === nodeId && node.step.step_name !== ignoreStep);
        if (currentNode && currentNode.input_files) {
            currentNode.input_files.forEach(inputNode => {
                inputNodes.push({uuid: inputNode.uuid, level: nodeDepth});
                inputNodes.push(...findInputNodes(allNodes, inputNode.uuid, depth - 1, ignoreStep, nodeDepth + 1));
            });
        }
    }
    return inputNodes;
}

function findOutputNodes(allNodes, nodeId, depth, ignoreStep, nodeDepth = 1) {
    let outputNodes = [];
    if (depth > 0) {
        allNodes.forEach(node => {
            if (node.input_files) {
                if (node.input_files.some(inputFile => inputFile.uuid === nodeId && node.step.step_name !== ignoreStep)) {
                    outputNodes.push({uuid: node.uuid, level: nodeDepth});
                    console.log(`Found ${node.uuid} as depth of ${nodeDepth}`);
                    outputNodes.push(...findOutputNodes(allNodes, node.uuid, depth - 1, ignoreStep, nodeDepth + 1));
                }
            }
        });
    }
    return outputNodes;
}


function show_details(jsonData, clickedNodeId) {
    let clickedNode = jsonData.files.find(file => file.uuid === clickedNodeId);
    console.log(clickedNode)

    // Display the clicked node data in the #file-details div
    let fileDetailsDiv = document.getElementById('file-details');
    fileDetailsDiv.innerHTML = `
        <h2>File Details</h2>
        <p><strong>File Name:</strong> ${clickedNode.file_name}.${clickedNode.file_type}</p>
        <p><strong>UUID:</strong> ${clickedNode.uuid}</p>
        <p><strong>Step:</strong></p>
        <ul>
          <li><strong>Path:</strong> ${clickedNode.step.path}</li>
          <li><strong>Step Name:</strong> ${clickedNode.step.step_name || "N/A"}</li>
          <li><strong>Version:</strong> ${clickedNode.step.version || "N/A"}</li>
        </ul>
        <p><strong>Columns:</strong></p>
        <ul>
          ${clickedNode.columns.map(column => `<li><strong>${column.name}:</strong> ${column.type}</li>`).join('')}
        </ul>
        <p><strong>Export Method Used:</strong> ${clickedNode.export_method_used}</p>
      `;
}

