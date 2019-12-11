var readline = require("readline-sync");
var MongoClient = require("mongodb").MongoClient;

var mapperCPU = function () {
    emit("CPUUtilization_Average", this.CPUUtilization_Average);
};

var mapperNetworkIn = function () {
    emit("NetworkIn_Average", this.NetworkIn_Average);
};

var mapperNetworkOut = function () {
    emit("NetworkOut_Average", this.NetworkOut_Average);
};

var mapperMemory = function () {
    emit("MemoryUtilization_Average", this.MemoryUtilization_Average);
};

var mapperFinal = function () {
    emit("Final_Target", this.Final_Target);
};

var reducerMin = function (key, values) {
    values = Array.sort(values);
    return values[0];
};

var reducerMax = function (key, values) {
    values = Array.sort(values);
    return values.pop();
};

var reducerAggregator = function(key, values){
    values = Array.sort(values);
    let last = values[values.length - 1];
    let isFirst = true;

    if(typeof last === 'object'){
        print(last.toSource());
        returning_object = last;
        values.pop();
        isFirst = false;
    }

    if(isFirst){
        let obj = {"values": values};
        return obj;
    }else{
        returning_object['values'].push(...values);
        return returning_object;
    }

}

var finalizerMedian = function(key, reducedValue){
    let values = Array.sort(reducedValue.values);

    const index = (values.length - 1) / 2;
    return values[index | 0];
}

var finalizerStdDev = function(key, reducedValue){
    let values = Array.sort(reducedValue.values);

    const reducer = (accumulator, value) => accumulator + value;
    const sum = values.reduce(reducer, 0);

    const average = sum / values.length;

    const reducerVariance = (accumulator, value) =>
        accumulator + Math.pow(value - average, 2);

    let variance = values.reduce(reducerVariance, 0);

    return Math.sqrt(variance / values.length);
};


var finalizerNorm = function(key, reducedValue){
    let values = Array.sort(reducedValue.values);
    const min = values[0];
    let max = values[values.length - 1];

    print('In Finalizer');

    const normalize = values.map((value) => 
    {
        let norm_val =  (parseFloat(value) - parseFloat(min)) / (parseFloat(max) - parseFloat(min));
        return norm_val;
    });

    return {"normalized_values" : normalize};
};


var finalizer90 = function(key, reducedValue){
    let values = Array.sort(reducedValue.values);

    return values[parseInt(0.9 * values.length)];
};

run().then(()=>{});

async function run() {
    let field = getField();
    let action = getAction();

    let map = getMapper(field);
    let reduce = getReducer(action);

    let collectionName = getCollection();

    console.log("Connecting...");
    client = await MongoClient.connect("mongodb://localhost:27017/ass2", {
        useUnifiedTopology: true
    });
    console.log("Connected");
    let collection = client.db("ass2").collection(collectionName);

    let configObject = { out: { inline: 1 }, verbose: true }

    if(action == 3){
        configObject.finalize = finalizerMedian;
    }else if(action == 4){
        configObject.finalize = finalizerStdDev;
    }else if(action == 5){
        configObject.finalize = finalizerNorm;
    }else if(action == 6){
        configObject.finalize = finalizer90;
    }

    collection.mapReduce(
        map,
        reduce,
        configObject,
        function (err, result, stats) {
            if(err){
                console.log("Error: ");
                console.log(err);
                process.exit(1); 
            }
            console.dir(result.results, {depth: null, colors: true, maxArrayLength: null});
            console.dir(result.stats, {depth: null, colors: true, maxArrayLength: null});

            client.close();

            console.log("Done");
        }
    );
}

function getField() {
    let field = readline.question(
        "Which field to analyze: " +
        "\n1)CPU Utilization Average" +
        "\n2)Network In Average" +
        "\n3)Network Out Average" +
        "\n4)Memory Utilization Average" +
        "\n5)Final_Target" +
        "\nEnter: "
    );

    if (parseInt(field) < 1 || parseInt(field) > 5) {
        console.log("Wrong selection");
        process.exit(1);
    }

    field = parseInt(field);

    return field;
}

function getAction() {
    let action = readline.question(
        "Which action to perform: " +
        "\n1)Min" +
        "\n2)Max" +
        "\n3)Median" +
        "\n4)Standard Deviation" +
        "\n5)Normalization" +
        "\n6)90-th percentile" +
        "\nEnter: "
    );

    if (parseInt(action) < 1 || parseInt(action) > 6) {
        console.log("Wrong selection");
        process.exit(1);
    }

    action = parseInt(action);

    return action;
}

function getCollection() {
    let collection = readline.question(
        "Which collection to use: " +
        "\n1)DVD-testing" +
        "\n2)DVD-training" +
        "\n3)NDBench-testing" +
        "\n4)NDBench-training" +
        "\nEnter: "
    );

    if (parseInt(collection) < 1 || parseInt(collection) > 6) {
        console.log("Wrong selection");
        process.exit(1);
    }

    collection = parseInt(collection);

    if (collection == 1) {
        return "DVD-testing";
    } else if (collection == 2) {
        return "DVD-training";
    } else if (collection == 3) {
        return "NDBench-testing";
    } else if (collection == 4) {
        return "NDBench-training";
    } else {
        console.log("Wrong selection");
        process.exit(1);
    }

    return collection;
}

function getMapper(field) {
    if (field == 1) {
        return mapperCPU;
    } else if (field == 2) {
        return mapperNetworkIn;
    } else if (field == 3) {
        return mapperNetworkOut;
    } else if (field == 4) {
        return mapperMemory;
    } else if (field == 5) {
        return mapperFinal;
    }
}

function getReducer(action) {
    if (action == 1) {
        return reducerMin;
    } else if (action == 2) {
        return reducerMax;
    } else if (action == 3) {
        return reducerAggregator;
    } else if (action == 4) {
        return reducerAggregator;
    } else if (action == 5) {
        return reducerAggregator;
    } else if (action == 6) {
        return reducerAggregator;
    }
}
