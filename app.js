const AWS = require('aws-sdk');
const config = require('./config');

AWS.config.update(config);
const docClient = new AWS.DynamoDB.DocumentClient();

const params = {
    TableName: "Cornell_Dairy_ESP",
    FilterExpression: '#time BETWEEN :start AND :end',
    ExpressionAttributeNames: {
        '#time': 'time',
    },
    ExpressionAttributeValues: {
        ':start': '2019-10-10T18:00',
        ':end': '2019-10-10T18:10'
    },
};

docClient.scan(params, onScan);
let result = [];

function onScan(err, data) {
    if (err) {
        console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
    } else {        
        console.log("Scan succeeded.");
        data.Items.forEach(function(itemdata) {
            result.push(itemdata);
            console.log("Item :",JSON.stringify(itemdata));
        });

        // continue scanning if we have more items
        if (typeof data.LastEvaluatedKey != "undefined") {
            console.log("Scanning for more...");
            params.ExclusiveStartKey = data.LastEvaluatedKey;
            docClient.scan(params, onScan);
        } else {
            let map = {};
            result.map(scan => {
                if (scan.esp in map) {
                    map[scan.esp]++;
                } else {
                    map[scan.esp] = 1;
                }
            })
            
            console.log(map);
        }
    }
}