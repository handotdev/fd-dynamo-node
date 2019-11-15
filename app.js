const AWS = require('aws-sdk');
const config = require('./config');
const date = require('date-and-time');

AWS.config.update(config);
const docClient = new AWS.DynamoDB.DocumentClient();



function scanRFIDBetween(start, end, callback) {
    const params = {
        TableName: "Cornell_Dairy_ESP",
        FilterExpression: '#sensor = :mac and #time BETWEEN :start AND :end',
        ExpressionAttributeNames: {
            '#sensor': 'esp',
            '#time': 'time'
        },
        ExpressionAttributeValues: {
            ':mac': '489315286F24',
            ':start': start,
            ':end': end
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
                callback(map);
            }
        }
    }
}

function timeInterval(time, before, after) {
    // @time: time in an ISO string format
    // @before: minutes before time
    // @after: minutes after time
    // return an array with arr[0] as the time string before and arr[1] as
    // the time string after

    const beforeTime =  new Date(Date.parse(time) - before*60000).toISOString();
    const afterTime =  new Date(Date.parse(time) + after*60000).toISOString();
    return [beforeTime.split('.')[0]+"Z", afterTime.split('.')[0]+"Z"];
}

function queryDistanceBetween() {
    const params = {
        TableName: "Cornell_Dairy_Eat",
        KeyConditionExpression: "#sensor = :mac and #time BETWEEN :start AND :end",
        ExpressionAttributeNames: {
            '#sensor': 'esp',
            '#time': 'time'
        },
        ExpressionAttributeValues: {
            ':mac': '489315286F24',
            ':start': '2019-10-10T08:00',
            ':end': '2019-10-10T18:00'
        },
    };

    docClient.query(params, (err, data) => {
        if (err) throw err;

        // data.Items.map(scan => {
        //     const interval = timeInterval(scan.time, 0.1, 0.1);
        //     console.log(scanRFIDBetween(interval[0], interval[1]));
        // })
        const items = data.Items;
        items.map((scan, i) => {
            if (i%100 === 0) {
                console.log(i);
                const interval = timeInterval(scan.time, 60, 60);
                scanRFIDBetween(interval[0], interval[1], (res) => {
                    console.log(res);
                });
            }
        })


    })
}

queryDistanceBetween();

// scanRFIDBetween('2019-10-10T10:00', '2019-10-10T11:00', (res) => {
//     console.log(res);
// });