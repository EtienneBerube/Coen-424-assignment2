const lineReader = require('line-reader');
var MongoClient = require('mongodb').MongoClient;
var path = require('path');
var assert = require('assert');
var db;

async function initDB(){
    console.log('Connecting...');
    client = await MongoClient.connect("mongodb://localhost:27017/ass2",{useUnifiedTopology: true});
    console.log('Connected');
    db = client.db('ass2');
    return db; 
}


function sendToDb(object, collection){
    db.collection(collection).insertOne(object);
}

function parseLine(line, collection){
    
    const values = line.split(',');
    if(values[0] === 'CPUUtilization_Average'){
        return; //First line
    }else{
        const obj = {
            'CPUUtilization_Average': parseFloat(values[0]),
            'NetworkIn_Average': parseFloat(values[1]),
            'NetworkOut_Average': parseFloat(values[2]),
            'MemoryUtilization_Average': parseFloat(values[3]),
            'Final_Target': parseFloat(values[4])
        };

        sendToDb(obj,collection );
    }
}

initDB().then((db) => {
    console.log('Starts reading...');
    lineReader.eachLine('resources/DVD-testing.csv', { encoding: 'utf8'}, (line) => {
        parseLine(line, 'DVD-testing');
    });
    
    lineReader.eachLine('resources/DVD-training.csv', {encoding: 'utf8'},  (line) => {
        parseLine(line, 'DVD-training');
    });
    
    lineReader.eachLine('resources/NDBench-testing.csv', {encoding: 'utf8'},  (line) => {
        parseLine(line, 'NDBench-testing');
    });
    
    lineReader.eachLine('resources/NDBench-training.csv', {encoding: 'utf8'},  (line) => {
        parseLine(line, 'NDBench-training');
    });
});

