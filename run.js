var readline = require('readline-sync');

run().then((data) => {
    console.log(data);
})

async function run(){
    const field = getField();
    const action = getAction();

    const map = getMapper(field);
    const reduce = getReducer(action);

    const collectionName = getCollection();

    console.log('Connecting...');
    client = await MongoClient.connect("mongodb://localhost:27017/ass2",{useUnifiedTopology: true});
    console.log('Connected');
    let collection = client.db('ass2').collection(collectionName);

    

    collection.mapReduce(map, reduce, {out : {inline: 1}, verbose:true}, function(err, results, stats) {
        console.log(results)
        console.log(stats)

        client.close();
      });
}

function getField() {
    const field = readline.question("Which field to analyze: "
    +"\n1)CPU Utilization Average" 
    +"\n2)Network In Average" 
    +"\n3)Network Out Average"
    +"\n4)Memory Utilization Average"
    +"\n5)Final_Target");

    if(parseInt(field) < 1 || parseInt(field) > 5){
        console.log('Wrong selection')
        process.exit(1);  
    }

    return field

}

function getAction() {
    const action = readline.question("Which action to perform: "
    +"\n1)Min" 
    +"\n2)Max" 
    +"\n3)Average"
    +"\n4)Standard Deviation"
    +"\n5)Normalization"
    +"\n6)90-th percentile");

    if(parseInt(action) < 1 || parseInt(action) > 6){
        console.log('Wrong selection')
        process.exit(1);  
    }
    return action
}

function getCollection() {
    const collection = readline.question("Which collection to use: "
    +"\n1)DVD-testing" 
    +"\n2)DVD-training" 
    +"\n3)NDBench-testing"
    +"\n4)NDBench-training");

    if(parseInt(collection) < 1 || parseInt(collection) > 6){
        console.log('Wrong selection')
        process.exit(1);  
    }

    if(collection == 1){
        return 'DVD-testing';
    }else if(collection == 2){
        return 'DVD-training';
    }else if(collection == 3){
        return 'NDBench-testing';
    }else if(collection == 4){
        return 'NDBench-training';
    }else{
        console.log('Wrong selection')
        process.exit(1);    
    }

    return collection
}

function getMapper(field){
    if(field == 1){
        return function(){
            emit('CPUUtilization_Average', this.CPUUtilization_Average);
        }
    }else if(field == 2){
        return function(){
            emit('NetworkIn_Average', this.NetworkIn_Average);
        }
    }else if(field == 3){
        return function(){
            emit('NetworkOut_Average', this.NetworkOut_Average);
        }
    }else if(field == 4){
        return function(){
            emit('MemoryUtilization_Average', this.MemoryUtilization_Average);
        }
    }else if(field == 5){
        return function(){
            emit('Final_Target', this.Final_Target);
        }
    }
}

function getReducer(action){

}

// function mapperCPU(){
//     emit('CPUUtilization_Average', this.CPUUtilization_Average);
// }

// function mapperNetworkIn(){
//     emit('NetworkIn_Average', this.NetworkIn_Average);
// }

// function mapperNetworkOut(){
//     emit('NetworkOut_Average', this.NetworkOut_Average);
// }

// function mapperMemory(){
//     emit('MemoryUtilization_Average', this.MemoryUtilization_Average);
// }

// function mapperFinal(){
//     emit('Final_Target', this.Final_Target);
// }